package org.apache.avro.compatibility;

import org.apache.avro.Schema;

/**
 * Reader/writer schema pair that can be used as a key in a hash map.
 *
 * This reader/writer pair differentiates Schema objects based on their system hash code.
 */
final class ReaderWriter {
  private final Schema mReader;
  private final Schema mWriter;

  /**
   * Initializes a new reader/writer pair.
   *
   * @param reader Reader schema.
   * @param writer Writer schema.
   */
  public ReaderWriter(final Schema reader, final Schema writer) {
    mReader = reader;
    mWriter = writer;
  }

  /**
   * Returns the reader schema in this pair.
   * @return the reader schema in this pair.
   */
  public Schema getReader() {
    return mReader;
  }

  /**
   * Returns the writer schema in this pair.
   * @return the writer schema in this pair.
   */
  public Schema getWriter() {
    return mWriter;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return System.identityHashCode(mReader) ^ System.identityHashCode(mWriter);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ReaderWriter)) {
      return false;
    }
    final ReaderWriter that = (ReaderWriter) obj;
    // Use pointer comparison here:
    return (this.mReader == that.mReader)
        && (this.mWriter == that.mWriter);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return String.format("ReaderWriter{reader:%s, writer:%s}", mReader, mWriter);
  }
}
