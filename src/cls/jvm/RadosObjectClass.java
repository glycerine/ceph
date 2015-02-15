import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

class RadosObjectClass {

  /*
   * SDK
   */

  interface BufferList {
    ByteBuffer getBytes();
    void append(BufferList bl);
    int length();
  }

  interface Context {
    BufferList getInput();
    BufferList getOutput();

    void log(int level, String msg);
    void remove();
    void create(boolean exclusive);

    BufferList read(int offset, int length);
    void write(int offset, int length, BufferList bl);
  }

  /*
   * Example ObjectClass
   */

  static class MyObjectClass {
    static void Echo(Context ctx) {
      // EchoParams
      BufferList output = ctx.getOutput();
      output.append(ctx.getInput());

      // EchoWrite
      //BufferList output = ctx.getOutput();
      //BufferList data = ctx.read(0, 0);
      //output.append(data);

      // EchoRead
      //BufferList input = ctx.getInput();
      //ctx.write(0, input.length(), input);
    }
  }

  static class BufferListImpl implements BufferList {

    long handle;

    BufferListImpl(long inbl) {
      this.handle = inbl;
    }

    public ByteBuffer getBytes() {
      return bl_get_bytebuffer(handle);
    }

    public void append(BufferList bl) {
      assert bl instanceof BufferListImpl;
      bl_append(handle, ((BufferListImpl)bl).handle);
    }

    public int length() {
      return bl_get_length(handle);
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

    public BufferList getOutput() {
      return new BufferListImpl(outbl);
    }

    public void log(int level, String msg) {
      RadosObjectClass.cls_log(level, msg);
    }

    public void remove() {
      RadosObjectClass.cls_remove(hctxp);
    }

    public void create(boolean exclusive) {
      RadosObjectClass.cls_create(hctxp, exclusive);
    }

    public BufferList read(int offset, int length) {
      long bl = RadosObjectClass.cls_read(hctxp, offset, length);
      return new BufferListImpl(bl);
    }

    public void write(int offset, int length, BufferList bl) {
      assert bl instanceof BufferListImpl;
      RadosObjectClass.cls_write(hctxp, offset, length,
          ((BufferListImpl)bl).handle);
    }
  }

  private static int cls_handle_wrapper(final long hctx,
      final long inbl, final long outbl) {

    Context ctx = new ContextImpl(hctx, inbl, outbl);

    MyObjectClass.Echo(ctx);

    cls_log(0, "ptr = " + inbl);
    return 0;
  }

  private RadosObjectClass() {}

  private static native void cls_log(int level, String msg);
  private static native void cls_remove(long ctx);
  private static native void cls_create(long ctx, boolean excl);
  private static native long cls_read(long ctx, int offset, int length);
  private static native void cls_write(long ctx, int offset, int length, long bl);

  private static native ByteBuffer bl_get_bytebuffer(long handle);
  private static native void bl_append(long dst, long src);
  private static native int bl_get_length(long handle);
}
