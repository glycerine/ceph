
#include <errno.h>
#include <jni.h>

#include "common/errno.h"
#include "objclass/objclass.h"
  
CLS_VER(1,0)
CLS_NAME(jvm)

cls_handle_t h_class;
cls_method_handle_t h_test;

static JavaVM *jvm;
static jclass wrapper_cls;
static jmethodID wrapper_mid;

static JNIEnv *getJniEnv() {
  JNIEnv *env;
  jint rs = jvm->AttachCurrentThread(reinterpret_cast<void **>(&env), NULL);
  assert(rs == JNI_OK);
  return env;
}

static int test(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  JNIEnv *env = getJniEnv();

  jlong jhctx = reinterpret_cast<jlong>(hctx);
  jlong jout = reinterpret_cast<jlong>(out);

  jobject jin = env->NewDirectByteBuffer(in->c_str(), in->length());
  if (!jin) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    CLS_LOG(0, "ERROR: failed to allocate direct byte buffer");
    return -EIO;
  }

  jint ret = env->CallStaticIntMethod(wrapper_cls, wrapper_mid, jhctx, jin, jout);
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
  }

  CLS_LOG(0, "cls got native %d", ret);

  jvm->DetachCurrentThread();

  return 0;
}

static void native_bl_append(JNIEnv *env, jclass clazz, jlong jblp, jobject jbuf)
{
  bufferlist *bl = reinterpret_cast<bufferlist*>(jblp);
  bl->append(
      static_cast<char*>(env->GetDirectBufferAddress(jbuf)),
      static_cast<jlong>(env->GetDirectBufferCapacity(jbuf))
  );
}

static void native_bl_clear(JNIEnv *env, jclass clazz, jlong jblp)
{
  bufferlist *bl = reinterpret_cast<bufferlist*>(jblp);
  bl->clear();
}

static jint native_bl_size(JNIEnv *env, jclass clazz, jlong jblp)
{
  bufferlist *bl = reinterpret_cast<bufferlist*>(jblp);
  return static_cast<jint>(bl->length());
}

/*
 *
 */
static void native_cls_remove(JNIEnv *env, jclass clazz, jlong jhctx)
{
  cls_method_context_t hctx = reinterpret_cast<cls_method_context_t>(jhctx);
  int ret = cls_cxx_remove(hctx);
  CLS_LOG(0, "jvm_remove: %d", ret);
}

/*
 *
 */
static void native_cls_create(JNIEnv *env, jclass clazz, jlong jhctx, jboolean jexclusive)
{
  cls_method_context_t hctx = reinterpret_cast<cls_method_context_t>(jhctx);
  int ret = cls_cxx_create(hctx, static_cast<bool>(jexclusive));
  CLS_LOG(0, "jvm_create: %d", ret);
}

/*
 *
 */
static void cls_jvm_vfprintf(FILE *fp, const char *format, va_list ap)
{
  char buf[4096];
  vsnprintf(buf, sizeof(buf), format, ap);
  CLS_LOG(0, "%s", buf);
}

/*
 *
 */
static void jni_cls_log(JNIEnv *env, jclass clazz, jint jlevel, jstring jmsg)
{
  const char *msg = env->GetStringUTFChars(jmsg, NULL);
  cls_log(static_cast<int>(jlevel), "%s", msg);
  env->ReleaseStringUTFChars(jmsg, msg);
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

  options[0].optionString = (char*)"-Djava.class.path=/usr/lib/java:/tmp";
  options[1].optionString = (char*)"vfprintf";
  options[1].extraInfo = (void*)cls_jvm_vfprintf;

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

  std::vector<JNINativeMethod> native_methods;
#define ADD_NATIVE_METHOD(n, s, fnp) do { \
    JNINativeMethod __m; \
    __m.name = (char*)(n); \
    __m.signature = (char*)(s); \
    __m.fnPtr = (void*)(fnp); \
    native_methods.push_back(__m); \
  } while (0)

  ADD_NATIVE_METHOD("cls_log", "(ILjava/lang/String;)V", jni_cls_log);
  ADD_NATIVE_METHOD("native_cls_remove", "(J)V", native_cls_remove);
  ADD_NATIVE_METHOD("native_cls_create", "(JZ)V", native_cls_create);

  ADD_NATIVE_METHOD("native_bl_clear", "(J)V", native_bl_clear);
  ADD_NATIVE_METHOD("native_bl_size", "(J)I", native_bl_size);
  ADD_NATIVE_METHOD("native_bl_append", "(JLjava/nio/ByteBuffer;)V", native_bl_append);


  ret = env->RegisterNatives(wrapper_cls, &native_methods[0], native_methods.size());
  if (ret) {
    CLS_LOG(0, "ERROR: failed to register natives %d", ret);
    return;
  }

  wrapper_mid = env->GetStaticMethodID(wrapper_cls,
      "cls_handle_wrapper", "(JLjava/nio/ByteBuffer;J)I");
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

  cls_register_cxx_method(h_class, "test",
      CLS_METHOD_RD | CLS_METHOD_WR,
      test, &h_test);
}
