package org.apache.avro.compatibility;

public enum SchemaIncompatibilityType {
  NAME_MISMATCH,
  FIXED_SIZE_MISMATCH,
  MISSING_ENUM_SYMBOLS,
  READER_FIELD_MISSING_DEFAULT_VALUE,
  TYPE_MISMATCH,
  MISSING_UNION_BRANCH;
}
