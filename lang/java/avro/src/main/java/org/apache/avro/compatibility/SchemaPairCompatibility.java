package org.apache.avro.compatibility;

import java.util.Arrays;

import org.apache.avro.Schema;

/**
 * Provides information about the compatibility of a single reader and writer schema pair.
 *
 * Note: This class represents a one-way relationship from the reader to the writer schema.
 */
public final class SchemaPairCompatibility {
  /** The details of this result. */
  private final SchemaCompatibilityResult mResult;

  /** Validated reader schema. */
  private final Schema mReader;

  /** Validated writer schema. */
  private final Schema mWriter;

  /** Human readable description of this result. */
  private final String mDescription;

  /**
   * Constructs a new instance.
   * @param result The result of the compatibility check.
   * @param type of the schema compatibility.
   * @param reader schema that was validated.
   * @param writer schema that was validated.
   * @param description of this compatibility result.
   */
  public SchemaPairCompatibility(
      SchemaCompatibilityResult result,
      Schema reader,
      Schema writer,
      String description) {
    mResult = result;
    mReader = reader;
    mWriter = writer;
    mDescription = description;
  }

  /**
   * Gets the type of this result.
   *
   * @return the type of this result.
   */
  public SchemaCompatibility.SchemaCompatibilityType getType() {
    return mResult.getCompatibility();
  }

  /**
   * Gets more details about the compatibility, in particular if getType() is INCOMPATIBLE.
   * @return the details of this compatibility check.
   */
  public SchemaCompatibilityResult getResult() {
    return mResult;
  }

  /**
   * Gets the reader schema that was validated.
   *
   * @return reader schema that was validated.
   */
  public Schema getReader() {
    return mReader;
  }

  /**
   * Gets the writer schema that was validated.
   *
   * @return writer schema that was validated.
   */
  public Schema getWriter() {
    return mWriter;
  }

  /**
   * Gets a human readable description of this validation result.
   *
   * @return a human readable description of this validation result.
   */
  public String getDescription() {
    return mDescription;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return String.format(
        "SchemaPairCompatibility{result:%s, readerSchema:%s, writerSchema:%s, description:%s}",
        mResult, mReader, mWriter, mDescription);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if ((null != other) && (other instanceof SchemaPairCompatibility)) {
      final SchemaPairCompatibility result = (SchemaPairCompatibility) other;
      return SchemaCompatibility.objectsEqual(result.mResult, mResult)
          && SchemaCompatibility.objectsEqual(result.mReader, mReader)
          && SchemaCompatibility.objectsEqual(result.mWriter, mWriter)
          && SchemaCompatibility.objectsEqual(result.mDescription, mDescription);
    } else {
      return false;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[]{mResult, mReader, mWriter, mDescription});
  }
}
