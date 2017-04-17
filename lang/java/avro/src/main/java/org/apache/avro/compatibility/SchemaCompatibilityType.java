package org.apache.avro.compatibility;

/**
 * Identifies the type of a schema compatibility result.
 */
public enum SchemaCompatibilityType {
  COMPATIBLE,
  INCOMPATIBLE,

  /** Used internally to tag a reader/writer schema pair and prevent recursion. */
  RECURSION_IN_PROGRESS;
}
