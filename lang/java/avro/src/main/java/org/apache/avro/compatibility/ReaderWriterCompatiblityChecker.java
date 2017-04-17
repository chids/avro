package org.apache.avro.compatibility;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines the compatibility of a reader/writer schema pair.
 *
 * <p> Provides memoization to handle recursive schemas. </p>
 */
final class ReaderWriterCompatiblityChecker {
  private static final Logger LOG = LoggerFactory.getLogger(ReaderWriterCompatiblityChecker.class);
  private static final String ROOT_REFERENCE_TOKEN = "";
  private final Map<ReaderWriter, SchemaCompatibilityResult> mMemoizeMap =
      new HashMap<ReaderWriter, SchemaCompatibilityResult>();

  /**
   * Reports the compatibility of a reader/writer schema pair.
   *
   * <p> Memoizes the compatibility results. </p>
   *
   * @param reader Reader schema to test.
   * @param writer Writer schema to test.
   * @return the compatibility of the reader/writer schema pair.
   */
  public SchemaCompatibilityResult getCompatibility(
      final Schema reader,
      final Schema writer
  ) {
    Stack<String> location = new Stack<String>();
    return getCompatibility(ROOT_REFERENCE_TOKEN, reader, writer, location);
  }

  /**
   * Reports the compatibility of a reader/writer schema pair.
   * <p>
   * Memoizes the compatibility results.
   * </p>
   * @param referenceToken The equivalent JSON pointer reference token representation of the schema node being visited.
   * @param reader Reader schema to test.
   * @param writer Writer schema to test.
   * @param location Stack with which to track the location within the schema.
   * @return the compatibility of the reader/writer schema pair.
   */
  private SchemaCompatibilityResult getCompatibility(
      String referenceToken,
      final Schema reader,
      final Schema writer,
      final Stack<String> location) {
    location.push(referenceToken);
    LOG.debug("Checking compatibility of reader {} with writer {}", reader, writer);
    final ReaderWriter pair = new ReaderWriter(reader, writer);
    final SchemaCompatibilityResult existing = mMemoizeMap.get(pair);
    if (existing != null) {
      if (existing.getCompatibility() == SchemaCompatibilityType.RECURSION_IN_PROGRESS) {
        // Break the recursion here.
        // schemas are compatible unless proven incompatible:
        location.pop();
        return SchemaCompatibilityResult.compatible();
      }
      return existing;
    }
    // Mark this reader/writer pair as "in progress":
    mMemoizeMap.put(pair, SchemaCompatibilityResult.recursionInProgress());
    final SchemaCompatibilityResult calculated = calculateCompatibility(reader, writer, location);
    if (calculated == SchemaCompatibilityResult.COMPATIBLE) {
      location.pop();
    }
    mMemoizeMap.put(pair, calculated);
    return calculated;
  }

  /**
   * Calculates the compatibility of a reader/writer schema pair.
   *
   * <p>
   * Relies on external memoization performed by {@link #getCompatibility(Schema, Schema)}.
   * </p>
   *
   * @param reader Reader schema to test.
   * @param writer Writer schema to test.
   * @param location Stack with which to track the location within the schema.
   * @return the compatibility of the reader/writer schema pair.
   */
  private SchemaCompatibilityResult calculateCompatibility(
      final Schema reader,
      final Schema writer,
      final Stack<String> location
  ) {
    assert (reader != null);
    assert (writer != null);

    if (reader.getType() == writer.getType()) {
      switch (reader.getType()) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING: {
          return SchemaCompatibilityResult.compatible();
        }
        case ARRAY: {
          return getCompatibility("items", reader.getElementType(), writer.getElementType(), location);
        }
        case MAP: {
          return getCompatibility("values", reader.getValueType(), writer.getValueType(), location);
        }
        case FIXED: {
          SchemaCompatibilityResult nameCheck = checkSchemaNames(reader, writer, location);
          if (nameCheck.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
            return nameCheck;
          }
          return checkFixedSize(reader, writer, location);
        }
        case ENUM: {
          SchemaCompatibilityResult nameCheck = checkSchemaNames(reader, writer, location);
          if (nameCheck.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
            return nameCheck;
          }
          return checkReaderEnumContainsAllWriterEnumSymbols(reader, writer, location);
        }
        case RECORD: {
          SchemaCompatibilityResult nameCheck = checkSchemaNames(reader, writer, location);
          if (nameCheck.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
            return nameCheck;
          }
          return checkReaderWriterRecordFields(reader, writer, location);
        }
        case UNION: {
          // Check that each individual branch of the writer union can be decoded:
          int i = 0;
          for (final Schema writerBranch : writer.getTypes()) {
            location.push(Integer.toString(i));
            SchemaCompatibilityResult compatibility = getCompatibility(reader, writerBranch);
            if (compatibility.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
              String message = String.format("reader union lacking writer type: %s",
                  writerBranch.getType());
              return SchemaCompatibilityResult.incompatible(
                  SchemaIncompatibilityType.MISSING_UNION_BRANCH,
                  reader, writer, message, location);
            }
            location.pop();
            i++;
          }
          // Each schema in the writer union can be decoded with the reader:
          return SchemaCompatibilityResult.compatible();
        }

        default: {
          throw new AvroRuntimeException("Unknown schema type: " + reader.getType());
        }
      }

    } else {
      // Reader and writer have different schema types:

      // Reader compatible with all branches of a writer union is compatible
      if (writer.getType() == Schema.Type.UNION) {
        int i = 0;
        for (Schema s : writer.getTypes()) {
          location.push(Integer.toString(i));
          SchemaCompatibilityResult result = getCompatibility(reader, s);
          if (result.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
            return result;
          }
          location.pop();
        }
        return SchemaCompatibilityResult.compatible();
      }

      switch (reader.getType()) {
        case NULL: return typeMismatch(reader, writer, location);
        case BOOLEAN: return typeMismatch(reader, writer, location);
        case INT: return typeMismatch(reader, writer, location);
        case LONG: {
          return (writer.getType() == Schema.Type.INT)
              ? SchemaCompatibilityResult.compatible()
              : typeMismatch(reader, writer, location);
        }
        case FLOAT: {
          return ((writer.getType() == Schema.Type.INT)
              || (writer.getType() == Schema.Type.LONG))
              ? SchemaCompatibilityResult.compatible()
              : typeMismatch(reader, writer, location);

        }
        case DOUBLE: {
          return ((writer.getType() == Schema.Type.INT)
              || (writer.getType() == Schema.Type.LONG)
              || (writer.getType() == Schema.Type.FLOAT))
              ? SchemaCompatibilityResult.compatible()
              : typeMismatch(reader, writer, location);
        }
        case BYTES: {
            return (writer.getType() == Schema.Type.STRING)
                    ? SchemaCompatibilityResult.compatible()
                    : typeMismatch(reader, writer, location);
              }
        case STRING: {
            return (writer.getType() == Schema.Type.BYTES)
                ? SchemaCompatibilityResult.compatible()
                : typeMismatch(reader, writer, location);
          }

        case ARRAY: return typeMismatch(reader, writer, location);
        case MAP: return typeMismatch(reader, writer, location);
        case FIXED: return typeMismatch(reader, writer, location);
        case ENUM: return typeMismatch(reader, writer, location);
        case RECORD: return typeMismatch(reader, writer, location);
        case UNION: {
          for (final Schema readerBranch : reader.getTypes()) {
            SchemaCompatibilityResult compatibility = getCompatibility(readerBranch, writer);
            if (compatibility.getCompatibility() == SchemaCompatibilityType.COMPATIBLE) {
              return SchemaCompatibilityResult.compatible();
            }
          }
          // No branch in the reader union has been found compatible with the writer schema:
          String message = String.format("reader union lacking writer type: %s", writer.getType());
          return SchemaCompatibilityResult.incompatible(
              SchemaIncompatibilityType.MISSING_UNION_BRANCH,
              reader, writer, message, location);
        }

        default: {
          throw new AvroRuntimeException("Unknown schema type: " + reader.getType());
        }
      }
    }
  }

  private SchemaCompatibilityResult checkReaderWriterRecordFields(final Schema reader,
      final Schema writer,
      final Stack<String> location) {
    location.push("fields");
    // Check that each field in the reader record can be populated from
    // the writer record:
    for (final Schema.Field readerField : reader.getFields()) {
      location.push(Integer.toString(readerField.pos()));
      final Schema.Field writerField = SchemaCompatibility.lookupWriterField(writer, readerField);
      if (writerField == null) {
        // Reader field does not correspond to any field in the writer
        // record schema, so the reader field must have a default value.
        if (readerField.defaultValue() == null) {
          // reader field has no default value
          return SchemaCompatibilityResult.incompatible(
              SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE, reader, writer,
              readerField.name(), location);
        }
      } else {
        SchemaCompatibilityResult compatibility = getCompatibility("type", readerField.schema(),
            writerField.schema(), location);
        if (compatibility.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
          return compatibility;
        }
      }
      location.pop();
    }
    // All fields in the reader record can be populated from the writer
    // record:
    location.pop();
    return SchemaCompatibilityResult.compatible();
  }

  private SchemaCompatibilityResult checkReaderEnumContainsAllWriterEnumSymbols(
      final Schema reader, final Schema writer, final Stack<String> location) {
    location.push("symbols");
    final Set<String> symbols = new TreeSet<String>(writer.getEnumSymbols());
    symbols.removeAll(reader.getEnumSymbols());
    if (!symbols.isEmpty()) {
      return SchemaCompatibilityResult.incompatible(
          SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS, reader, writer,
          symbols.toString(), location);
    }
    location.pop();
    return SchemaCompatibilityResult.compatible();
  }

  private SchemaCompatibilityResult checkFixedSize(final Schema reader, final Schema writer, final Stack<String> location) {
    location.push("size");
    int actual = reader.getFixedSize();
    int expected = writer.getFixedSize();
    if (actual != expected) {
      String message = String.format("expected: %d, found: %d", expected, actual);
      return SchemaCompatibilityResult.incompatible(
          SchemaIncompatibilityType.FIXED_SIZE_MISMATCH, reader,
          writer, message, location);
    }
    location.pop();
    return SchemaCompatibilityResult.compatible();
  }

  private SchemaCompatibilityResult checkSchemaNames(final Schema reader, final Schema writer, final Stack<String> location) {
    location.push("name");
    if (!SchemaCompatibility.schemaNameEquals(reader, writer)) {
      String message = String.format("expected: %s", writer.getFullName());
      return SchemaCompatibilityResult.incompatible(
          SchemaIncompatibilityType.NAME_MISMATCH,
          reader, writer, message, location);
    }
    location.pop();
    return SchemaCompatibilityResult.compatible();
  }

  private SchemaCompatibilityResult typeMismatch(final Schema reader, final Schema writer, final Stack<String> location) {
    String message = String.format("reader type: %s not compatible with writer type: %s",
        reader.getType(), writer.getType());
    return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.TYPE_MISMATCH,
        reader, writer, message, location);
  }
}
