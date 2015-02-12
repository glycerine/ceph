import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

class RadosObjectClass {

  /*
   * SDK
   */

  interface BufferList {
    ByteBuffer getBytes();
  }

  interface Context {
    BufferList getInput();
    void Log(int level, String msg);
    void Remove();
    void Create(boolean exclusive);
  }

  /*
   * Example ObjectClass
   */

  static class MyObjectClass {
    static void Echo(Context ctx) {
    }
  }

  static class BufferListImpl implements BufferList {

    private long handle;

    BufferListImpl(long inbl) {
      this.handle = inbl;
    }

    public ByteBuffer getBytes() {
      return bl_get_bytebuffer(handle);
    }
  }

  static class ContextImpl implements Context {
    private long hctxp;
    private long inbl;
    private long outbl;

    ContextImpl(long hctxp, long inbl, long outbl) {
      this.hctxp = hctxp;
      this.inbl = inbl;
      this.outbl = outbl;
    }

    public BufferList getInput() {
      return new BufferListImpl(inbl);
    }

    public void Log(int level, String msg) {
      RadosObjectClass.cls_log(level, msg);
    }

    public void Remove() {
      RadosObjectClass.cls_remove(hctxp);
    }

    public void Create(boolean exclusive) {
      RadosObjectClass.cls_create(hctxp, exclusive);
    }
  }

  private static int cls_handle_wrapper(final long hctx,
      final long inbl, final long outbl) {

    Context ctx = new ContextImpl(hctx, inbl, outbl);

    MyObjectClass.Echo(ctx);

    cls_log(0, "ptr = " + inbl);
    return 3;
  }

  private RadosObjectClass() {}

  private static native void cls_log(int level, String msg);
  private static native void cls_remove(long ctx);
  private static native void cls_create(long ctx, boolean excl);

  private static native ByteBuffer bl_get_bytebuffer(long handle);
}
