// automatically generated by the FlatBuffers compiler, do not modify

package org.locationtech.geogig.flatbuffers.generated.v1.values;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;

@SuppressWarnings("unused")
public final class BOOLEAN extends Table {
  public static BOOLEAN getRootAsBOOLEAN(ByteBuffer _bb) { return getRootAsBOOLEAN(_bb, new BOOLEAN()); }
  public static BOOLEAN getRootAsBOOLEAN(ByteBuffer _bb, BOOLEAN obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public BOOLEAN __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public boolean value() { int o = __offset(4); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }

  public static int createBOOLEAN(FlatBufferBuilder builder,
      boolean value) {
    builder.startObject(1);
    BOOLEAN.addValue(builder, value);
    return BOOLEAN.endBOOLEAN(builder);
  }

  public static void startBOOLEAN(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addValue(FlatBufferBuilder builder, boolean value) { builder.addBoolean(0, value, false); }
  public static int endBOOLEAN(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}
