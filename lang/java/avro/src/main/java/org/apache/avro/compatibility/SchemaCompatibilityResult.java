package org.apache.avro.compatibility;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;

/**
 * Immutable class representing details about a particular schema pair
 * compatibility check.
 */
public final class SchemaCompatibilityResult {
  private final SchemaCompatibility.SchemaCompatibilityType mCompatibility;
  // the below fields are only valid if INCOMPATIBLE
  private final SchemaCompatibility.SchemaIncompatibilityType mSchemaIncompatibilityType;
  private final Schema mReaderSubset;
  private final Schema mWriterSubset;
  private final String mMessage;
  private final List<String> mLocation;
  // cached objects for stateless details
  static final SchemaCompatibilityResult COMPATIBLE = new SchemaCompatibilityResult(
      SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, null, null, null, null, null);
  private static final SchemaCompatibilityResult RECURSION_IN_PROGRESS = new SchemaCompatibilityResult(
      SchemaCompatibility.SchemaCompatibilityType.RECURSION_IN_PROGRESS, null, null, null, null, null);

  private SchemaCompatibilityResult(SchemaCompatibility.SchemaCompatibilityType type,
                                    SchemaCompatibility.SchemaIncompatibilityType errorDetails,
                                    Schema readerDetails, Schema writerDetails, String message, List<String> location) {
    this.mCompatibility = type;
    this.mSchemaIncompatibilityType = errorDetails;
    this.mReaderSubset = readerDetails;
    this.mWriterSubset = writerDetails;
    this.mMessage = message;
    this.mLocation = location;
  }

  /**
   * Returns a details object representing a compatible schema pair.
   * @return a SchemaCompatibilityResult object with COMPATIBLE
   *         SchemaCompatibilityType, and no other state.
   */
  public static SchemaCompatibilityResult compatible() {
    return COMPATIBLE;
  }

  /**
   * Returns a details object representing a state indicating that recursion
   * is in progress.
   * @return a SchemaCompatibilityResult object with RECURSION_IN_PROGRESS
   *         SchemaCompatibilityType, and no other state.
   */
  public static SchemaCompatibilityResult recursionInProgress() {
    return RECURSION_IN_PROGRESS;
  }

  /**
   * Returns a details object representing an incompatible schema pair,
   * including error details.
   * @return a SchemaCompatibilityResult object with INCOMPATIBLE
   *         SchemaCompatibilityType, and state representing the violating
   *         part.
   */
  public static SchemaCompatibilityResult incompatible(SchemaCompatibility.SchemaIncompatibilityType error,
                                                       Schema reader, Schema writer, String message, List<String> location) {
    return new SchemaCompatibilityResult(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE, error, reader,
        writer, message, location);
  }

  /**
   * Returns the SchemaCompatibilityType, always non-null.
   * @return a SchemaCompatibilityType instance, always non-null
   */
  public SchemaCompatibility.SchemaCompatibilityType getCompatibility() {
    return mCompatibility;
  }

  /**
   * If the compatibility is INCOMPATIBLE, returns the
   * SchemaIncompatibilityType (first thing that was incompatible), otherwise
   * null.
   * @return a SchemaIncompatibilityType instance, or null
   */
  public SchemaCompatibility.SchemaIncompatibilityType getIncompatibility() {
    return mSchemaIncompatibilityType;
  }

  /**
   * If the compatibility is INCOMPATIBLE, returns the first part of the
   * reader schema that failed compatibility check.
   * @return a Schema instance (part of the reader schema), or null
   */
  public Schema getReaderSubset() {
    return mReaderSubset;
  }

  /**
   * If the compatibility is INCOMPATIBLE, returns the first part of the
   * writer schema that failed compatibility check.
   * @return a Schema instance (part of the writer schema), or null
   */
  public Schema getWriterSubset() {
    return mWriterSubset;
  }

  /**
   * If the compatibility is INCOMPATIBLE, returns a human-readable message
   * with more details about what failed. Syntax depends on the
   * SchemaIncompatibilityType.
   * @see #getIncompatibility()
   * @return a String with details about the incompatibility, or null
   */
  public String getMessage() {
    return mMessage;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if ((null != other) && (other instanceof SchemaCompatibilityResult)) {
      final SchemaCompatibilityResult result = (SchemaCompatibilityResult) other;
      return SchemaCompatibility.objectsEqual(result.mMessage, mMessage)
              && SchemaCompatibility.objectsEqual(result.mReaderSubset, mReaderSubset)
              && SchemaCompatibility.objectsEqual(result.mWriterSubset, mWriterSubset)
              && SchemaCompatibility.objectsEqual(result.mCompatibility, mCompatibility)
              && SchemaCompatibility.objectsEqual(result.mLocation, mLocation);
    } else {
      return false;
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[]{mMessage, mReaderSubset, mCompatibility, mSchemaIncompatibilityType, mWriterSubset, mLocation});
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return String.format(
        "SchemaCompatibilityResult{compatibility:%s, type:%s, readerSubset:%s, writerSubset:%s, message:%s, location:%s}",
        mCompatibility, mSchemaIncompatibilityType, mReaderSubset, mWriterSubset, mMessage, getLocation());
  }

  /**
   * Returns a <a href="https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-08">JSON Pointer</a> describing
   * the node location within the schema's JSON document tree where the incompatibility was encountered.
   * @return JSON Pointer encoded as a string or {@code null} if there was no incompatibility.
   */
  public String getLocation() {
    if (mCompatibility != SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE) {
      return null;
    }
    StringBuilder s = new StringBuilder("/");
    boolean first = true;
    // ignore root element
    for (String coordinate : mLocation.subList(1, mLocation.size())) {
      if (first) {
        first = false;
      } else {
        s.append('/');
      }
      // Apply JSON pointer escaping.
      s.append(coordinate.replace("~", "~0").replace("/", "~1"));
    }
    return s.toString();
  }
}
