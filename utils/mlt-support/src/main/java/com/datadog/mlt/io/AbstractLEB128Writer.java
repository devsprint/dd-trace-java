package com.datadog.mlt.io;

import java.nio.charset.StandardCharsets;

abstract class AbstractLEB128Writer implements LEB128Writer {
  @Override
  public final LEB128Writer writeChar(char data) {
    writeChar(position(), data);
    return this;
  }

  @Override
  public final int writeChar(int offset, char data) {
    return writeLong(offset, data & 0x000000000000ffffL);
  }

  @Override
  public final LEB128Writer writeShort(short data) {
    writeShort(position(), data);
    return this;
  }

  @Override
  public final int writeShort(int offset, short data) {
    return writeLong(offset, data & 0x000000000000ffffL);
  }

  @Override
  public final LEB128Writer writeInt(int data) {
    writeInt(position(), data);
    return this;
  }

  @Override
  public final int writeInt(int offset, int data) {
    return writeLong(offset, data & 0x00000000ffffffffL);
  }

  @Override
  public final LEB128Writer writeLong(long data) {
    writeLong(position(), data);
    return this;
  }

  @Override
  public final int writeLong(int offset, long data) {
    if ((data & LEB128Writer.COMPRESSED_INT_MASK) == 0) {
      return writeByte(offset, (byte) (data & 0xff));
    }
    offset = writeByte(offset, (byte) (data | LEB128Writer.EXT_BIT));
    data >>= 7;
    if ((data & LEB128Writer.COMPRESSED_INT_MASK) == 0) {
      return writeByte(offset, (byte) data);
    }
    offset = writeByte(offset, (byte) (data | LEB128Writer.EXT_BIT));
    data >>= 7;
    if ((data & LEB128Writer.COMPRESSED_INT_MASK) == 0) {
      return writeByte(offset, (byte) data);
    }
    offset = writeByte(offset, (byte) (data | LEB128Writer.EXT_BIT));
    data >>= 7;
    if ((data & LEB128Writer.COMPRESSED_INT_MASK) == 0) {
      return writeByte(offset, (byte) data);
    }
    offset = writeByte(offset, (byte) (data | LEB128Writer.EXT_BIT));
    data >>= 7;
    if ((data & LEB128Writer.COMPRESSED_INT_MASK) == 0) {
      return writeByte(offset, (byte) data);
    }
    offset = writeByte(offset, (byte) (data | LEB128Writer.EXT_BIT));
    data >>= 7;
    if ((data & LEB128Writer.COMPRESSED_INT_MASK) == 0) {
      return writeByte(offset, (byte) data);
    }
    offset = writeByte(offset, (byte) (data | LEB128Writer.EXT_BIT));
    data >>= 7;
    if ((data & LEB128Writer.COMPRESSED_INT_MASK) == 0) {
      return writeByte(offset, (byte) data);
    }
    offset = writeByte(offset, (byte) (data | LEB128Writer.EXT_BIT));
    data >>= 7;
    if ((data & LEB128Writer.COMPRESSED_INT_MASK) == 0) {
      return writeByte(offset, (byte) data);
    }
    offset = writeByte(offset, (byte) (data | LEB128Writer.EXT_BIT));
    return writeByte(offset, (byte) (data >> 7));
  }

  @Override
  public final LEB128Writer writeFloat(float data) {
    writeFloat(position(), data);
    return this;
  }

  @Override
  public final LEB128Writer writeDouble(double data) {
    writeDouble(position(), data);
    return this;
  }

  @Override
  public final LEB128Writer writeBoolean(boolean data) {
    writeBoolean(position(), data);
    return this;
  }

  @Override
  public final int writeBoolean(int offset, boolean data) {
    return writeByte(offset, data ? (byte) 1 : (byte) 0);
  }

  @Override
  public final LEB128Writer writeByte(byte data) {
    writeByte(position(), data);
    return this;
  }

  @Override
  public final LEB128Writer writeBytes(byte... data) {
    writeBytes(position(), data);
    return this;
  }

  @Override
  public final LEB128Writer writeUTF(String data) {
    writeUTF(position(), data);
    return this;
  }

  @Override
  public final LEB128Writer writeUTF(byte[] data) {
    writeUTF(position(), data);
    return this;
  }

  @Override
  public final int writeUTF(int offset, String data) {
    return writeUTF(offset, data == null ? null : data.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public final int writeUTF(int offset, byte[] data) {
    int len = data == null ? 0 : data.length;
    int pos = writeInt(offset, len);
    if (len > 0) {
      pos = writeBytes(pos, data);
    }
    return pos;
  }

  @Override
  public final LEB128Writer writeCompactUTF(byte[] data) {
    writeCompactUTF(position(), data);
    return this;
  }

  @Override
  public final LEB128Writer writeCompactUTF(String data) {
    writeCompactUTF(position(), data);
    return this;
  }

  @Override
  public final int writeCompactUTF(int offset, byte[] data) {
    if (data == null) {
      return writeByte(offset, (byte) 0); // special NULL encoding
    }
    if (data.length == 0) {
      return writeByte(offset, (byte) 1); // special empty string encoding
    }
    int pos = writeByte(offset, (byte) 3); // UTF-8 string
    pos = writeInt(pos, data.length);
    pos = writeBytes(pos, data);
    return pos;
  }

  @Override
  public final int writeCompactUTF(int offset, String data) {
    return writeCompactUTF(offset, data.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public final LEB128Writer writeShortRaw(short data) {
    writeShortRaw(position(), data);
    return this;
  }

  @Override
  public final LEB128Writer writeIntRaw(int data) {
    writeIntRaw(position(), data);
    return this;
  }

  @Override
  public final LEB128Writer writeLongRaw(long data) {
    writeLongRaw(position(), data);
    return this;
  }

  @Override
  public final int length() {
    return adjustLength(position());
  }

  static int adjustLength(int length) {
    int extraLen = 0;
    do {
      extraLen = getPackedIntLen(length + extraLen);
    } while (getPackedIntLen(length + extraLen) != extraLen);
    return length + extraLen;
  }

  static int getPackedIntLen(long data) {
    if ((data & COMPRESSED_INT_MASK) == 0) {
      return 1;
    }
    data >>= 7;
    if ((data & COMPRESSED_INT_MASK) == 0) {
      return 2;
    }
    data >>= 7;
    if ((data & COMPRESSED_INT_MASK) == 0) {
      return 3;
    }
    data >>= 7;
    if ((data & COMPRESSED_INT_MASK) == 0) {
      return 4;
    }
    data >>= 7;
    if ((data & COMPRESSED_INT_MASK) == 0) {
      return 5;
    }
    data >>= 7;
    if ((data & COMPRESSED_INT_MASK) == 0) {
      return 6;
    }
    data >>= 7;
    if ((data & COMPRESSED_INT_MASK) == 0) {
      return 7;
    }
    data >>= 7;
    if ((data & COMPRESSED_INT_MASK) == 0) {
      return 8;
    }
    return 9;
  }
}
