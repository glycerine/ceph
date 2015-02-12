/*
 * Todo/Ideas:
 * - The OSD can proactively run GC during idle periods
 * - Checkout the various GC options (e.g. prefer young generation)
 * - Checkout all VM options. There are a lot.
 * - Avoid creating the VM in init() because this gives us more flexibility to
 *   construct things like a create/destroy VM method call which could fail,
 *   or to allow multiple VMs to exist and route based on a request parameter.
 */
#include <errno.h>
#include <jni.h>

#include "common/errno.h"
#include "objclass/objclass.h"
#include "common/config.h"
#include "global/global_context.h"
  
CLS_VER(1,0)
CLS_NAME(jvm)

cls_handle_t h_class;
cls_method_handle_t h_java_route;

static JavaVM *jvm;
static jclass wrapper_cls;
static jmethodID wrapper_mid;

static JNIEnv *getJniEnv() {
  JNIEnv *env;
  jint rs = jvm->AttachCurrentThread(reinterpret_cast<void **>(&env), NULL);
  assert(rs == JNI_OK);
  return env;
}

/*
 * JNI interface: cls_cxx_remove
 */
static void jni_cls_remove(JNIEnv *env, jclass clazz, jlong jhctx)
{
  cls_method_context_t hctx = reinterpret_cast<cls_method_context_t>(jhctx);
  int ret = cls_cxx_remove(hctx);
  CLS_LOG(0, "jvm_remove: %d", ret);
}

/*
 * JNI interface: cls_cxx_create
 */
static void jni_cls_create(JNIEnv *env, jclass clazz, jlong jhctx, jboolean jexclusive)
{
  cls_method_context_t hctx = reinterpret_cast<cls_method_context_t>(jhctx);
  int ret = cls_cxx_create(hctx, static_cast<bool>(jexclusive));
  CLS_LOG(0, "jvm_create: %d", ret);
}

/*
 * JNI interface: cls_log
 */
static void jni_cls_log(JNIEnv *env, jclass clazz, jint jlevel, jstring jmsg)
{
  const char *msg = env->GetStringUTFChars(jmsg, NULL);
  cls_log(static_cast<int>(jlevel), "%s", msg);
  env->ReleaseStringUTFChars(jmsg, msg);
}

/*
 * JNI interface: return a ByteBuffer backed by a bufferlist.
 */
static jobject jni_bl_get_bytebuffer(JNIEnv *env, jclass clazz, jlong jbl)
{
  bufferlist *bl = reinterpret_cast<bufferlist*>(jbl);

  jobject ret = env->NewDirectByteBuffer(bl->c_str(), bl->length());
  if (!ret) {
    env->ExceptionDescribe();
    CLS_LOG(0, "ERROR: failed to create direct byte buffer");
    return NULL;
  }

  return ret;
}

/*
 * JNI interface: append bufferlist to another bufferlist.
 */
static void jni_bl_append_bl(JNIEnv *env, jclass clazz, jlong jdst, jlong jsrc)
{
  bufferlist *dst = reinterpret_cast<bufferlist*>(jdst);
  bufferlist *src = reinterpret_cast<bufferlist*>(jsrc);
  dst->append(*src);
}

/*
 * Object class handle that routes requests to Java
 */
static int java_route(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  JNIEnv *env = getJniEnv();

  jlong jhctx = reinterpret_cast<jlong>(hctx);
  jlong jin = reinterpret_cast<jlong>(in);
  jlong jout = reinterpret_cast<jlong>(out);

  jint ret = env->CallStaticIntMethod(wrapper_cls, wrapper_mid, jhctx, jin, jout);
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
  }

  CLS_LOG(0, "cls got native %d", ret);

  jvm->DetachCurrentThread();

  return ret;
}

/*
 * JVM adapter that dumps log output to cls_log.
 *
 * TODO: the JVM assumes an output stream (see @fp parameter) and will call
 * this method multiple times while constructing a single message. Since we
 * don't do any buffering here, the OSD output can be difficult to read since
 * each call corresponds to a newline in the OSD log.
 */
static void jvm_vfprintf_callback(FILE *fp, const char *format, va_list ap)
{
  char buf[4096];
  vsnprintf(buf, sizeof(buf), format, ap);
  CLS_LOG(0, "%s", buf);
}

void __cls_init()
{
  CLS_LOG(0, "Loaded Java class!");

  JNIEnv *env;

  JavaVMInitArgs vm_args;
  vm_args.version = JNI_VERSION_1_6;
  vm_args.ignoreUnrecognized = JNI_FALSE;

  vm_args.nOptions = 2;
  JavaVMOption options[vm_args.nOptions];

  /*
   * Setup CLASSPATH
   */
  stringstream cp_ss;

  cp_ss << "-Djava.class.path=";
  cp_ss << g_conf->cls_jvm_classpath_default;

  if (!g_conf->cls_jvm_classpath_extra.empty())
    cp_ss << ":" << g_conf->cls_jvm_classpath_extra;

  string cp = cp_ss.str();
  options[0].optionString = (char*)cp.c_str();

  CLS_LOG(0, "setting classpath = %s", cp.c_str());

  /*
   * Setup JVM reporting
   */
  options[1].optionString = (char*)"vfprintf";
  options[1].extraInfo = (void*)jvm_vfprintf_callback;

  vm_args.options = options;

  jint ret = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
  if (ret != JNI_OK) {
    CLS_LOG(0, "ERROR: failed to create JVM");
    return;
  }

  wrapper_cls = env->FindClass("RadosObjectClass");
  if (!wrapper_cls) {
    CLS_LOG(0, "ERROR: failed to load RadosObjectClass");
    return;
  }

  /*
   * Register native methods
   */
  std::vector<JNINativeMethod> native_methods;

#define save_native_method(_name, _funcptr, _sig) do { \
    JNINativeMethod m; \
    m.name = (char *)(_name); \
    m.signature = (char *)(_sig); \
    m.fnPtr = (void *)(_funcptr); \
    native_methods.push_back(m); \
  } while (0)

  save_native_method("cls_log",    jni_cls_log,    "(ILjava/lang/String;)V");
  save_native_method("cls_remove", jni_cls_remove, "(J)V");
  save_native_method("cls_create", jni_cls_create, "(JZ)V");

  save_native_method("bl_get_bytebuffer", jni_bl_get_bytebuffer, "(J)Ljava/nio/ByteBuffer;");
  save_native_method("bl_append", jni_bl_append_bl, "(JJ)V");

#undef save_native_method

  ret = env->RegisterNatives(wrapper_cls, &native_methods[0], native_methods.size());
  if (ret) {
    CLS_LOG(0, "ERROR: failed to register natives %d", ret);
    return;
  }

  wrapper_mid = env->GetStaticMethodID(wrapper_cls, "cls_handle_wrapper", "(JJJ)I");
  if (!wrapper_mid) {
    CLS_LOG(0, "ERROR: failed to load wrapper");
    return;
  }

  // need global refs

#if 0
  jmethodID ctor = env->GetMethodID(env, cls, "<init>", "()V");
  if (!ctor) {
    CLS_LOG(0, "ERROR: failed to get constructor");
    return;
  }

  jobject obj = env->NewObject(env, cls, ctor);
  if (!obj) {
    CLS_LOG(0, "ERROR: failed to create object");
    return;
  }
#endif


  cls_register("jvm", &h_class);

  cls_register_cxx_method(h_class, "java_route",
      CLS_METHOD_RD | CLS_METHOD_WR,
      java_route, &h_java_route);
}
