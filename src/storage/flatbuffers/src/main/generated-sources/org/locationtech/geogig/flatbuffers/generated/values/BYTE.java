// automatically generated by the FlatBuffers compiler, do not modify

package org.locationtech.geogig.flatbuffers.generated.values;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class BYTE extends Table {
  public static BYTE getRootAsBYTE(ByteBuffer _bb) { return getRootAsBYTE(_bb, new BYTE()); }
  public static BYTE getRootAsBYTE(ByteBuffer _bb, BYTE obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public BYTE __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public byte value() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 0; }

  public static int createBYTE(FlatBufferBuilder builder,
      byte value) {
    builder.startObject(1);
    BYTE.addValue(builder, value);
    return BYTE.endBYTE(builder);
  }

  public static void startBYTE(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addValue(FlatBufferBuilder builder, byte value) { builder.addByte(0, value, 0); }
  public static int endBYTE(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

