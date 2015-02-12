import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

class RadosObjectClass {

  interface BufferList {
    void clear();
    int size();
  }

  interface Context {
    BufferList getInputBufferList();

    void Log(int level, String msg);
    void Remove();
    void Create(boolean exclusive);
  }

  static class MyObjectClass {
    static void Echo(Context ctx) {
      bufferlist bl = ctx.getInput();
      ctx.setOutput(bl);
    }
  }

  static class ContextImpl implements Context {
    private long hctxp;

    ContextImpl(long hctxp) {
      this.hctxp = hctxp;
    }

    public void Log(int level, String msg) {
      RadosObjectClass.cls_log(level, msg);
    }

    public void Remove() {
      RadosObjectClass.native_cls_remove(hctxp);
    }

    public void Create(boolean exclusive) {
      RadosObjectClass.native_cls_create(hctxp, exclusive);
    }
  }

  /*
   *
   */
  static class BufferListImpl implements BufferList {
    private long blp;

    BufferListImpl(long blp) {
      this.blp = blp;
    }

    void clear() {
      RadosObjectClass.native_bl_clear(blp);
    }

    int size() {
      return RadosObjectClass.native_bl_size(blp);
    }
  }

  private static int cls_handle_wrapper(final long context,
      final ByteBuffer input, final long outblp) {

    Context ctx = new ContextImpl(context);

    MyObjectClass.Echo(ctx);

    cls_log(0, "len = " + input.capacity());
    return 3;
  }

  private RadosObjectClass() {}

  private static native void cls_log(int level, String msg);

  private static native void native_cls_remove(long ctxp);
  private static native void native_cls_create(long hctxp, boolean exclusive);

  private static native void native_bl_clear(long blp);
  private static native int native_bl_size(long blp);
  private static native void native_bl_append(long blp, ByteBuffer buf);
}
