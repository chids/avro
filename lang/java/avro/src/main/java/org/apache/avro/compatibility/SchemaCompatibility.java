/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.compatibility;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluate the compatibility between a reader schema and a writer schema.
 * A reader and a writer schema are declared compatible if all datum instances of the writer
 * schema can be successfully decoded using the specified reader schema.
 */
public class SchemaCompatibility {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaCompatibility.class);

  /** Utility class cannot be instantiated. */
  private SchemaCompatibility() {
  }

  /** Message to annotate reader/writer schema pairs that are compatible. */
  public static final String READER_WRITER_COMPATIBLE_MESSAGE =
      "Reader schema can always successfully decode data written using the writer schema.";

  /**
   * Validates that the provided reader schema can be used to decode avro data written with the
   * provided writer schema.
   *
   * @param reader schema to check.
   * @param writer schema to check.
   * @return a result object identifying any compatibility errors.
   */
  public static SchemaPairCompatibility checkReaderWriterCompatibility(
      final Schema reader,
      final Schema writer
  ) {
    final SchemaCompatibilityResult compatibility =
        new ReaderWriterCompatiblityChecker()
            .getCompatibility(reader, writer);

    final String message;
    switch (compatibility.getCompatibility()) {
      case INCOMPATIBLE: {
        message = String.format(
            "Data encoded using writer schema:%n%s%n"
            + "will or may fail to decode using reader schema:%n%s%n",
            writer.toString(true),
            reader.toString(true));
        break;
      }
      case COMPATIBLE: {
        message = READER_WRITER_COMPATIBLE_MESSAGE;
        break;
      }
      default: throw new AvroRuntimeException("Unknown compatibility: " + compatibility);
    }

    return new SchemaPairCompatibility(
        compatibility,
        reader,
        writer,
        message);
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Tests the equality of two Avro named schemas.
   *
   * <p> Matching includes reader name aliases. </p>
   *
   * @param reader Named reader schema.
   * @param writer Named writer schema.
   * @return whether the names of the named schemas match or not.
   */
  public static boolean schemaNameEquals(final Schema reader, final Schema writer) {
    final String writerFullName = writer.getFullName();
    if (objectsEqual(reader.getFullName(), writerFullName)) {
      return true;
    }
    // Apply reader aliases:
    if (reader.getAliases().contains(writerFullName)) {
      return true;
    }
    return false;
  }

  /**
   * Identifies the writer field that corresponds to the specified reader field.
   *
   * <p> Matching includes reader name aliases. </p>
   *
   * @param writerSchema Schema of the record where to look for the writer field.
   * @param readerField Reader field to identify the corresponding writer field of.
   * @return the writer field, if any does correspond, or None.
   */
  public static Field lookupWriterField(final Schema writerSchema, final Field readerField) {
    assert (writerSchema.getType() == Type.RECORD);
    final List<Field> writerFields = new ArrayList<Field>();
    final Field direct = writerSchema.getField(readerField.name());
    if (direct != null) {
      writerFields.add(direct);
    }
    for (final String readerFieldAliasName : readerField.aliases()) {
      final Field writerField = writerSchema.getField(readerFieldAliasName);
      if (writerField != null) {
        writerFields.add(writerField);
      }
    }
    switch (writerFields.size()) {
      case 0: return null;
      case 1: return writerFields.get(0);
      default: {
        throw new AvroRuntimeException(String.format(
            "Reader record field %s matches multiple fields in writer record schema %s",
            readerField, writerSchema));
      }
    }
  }

  /**
   * Identifies the type of a schema compatibility result.
   */
  public enum SchemaCompatibilityType {
    COMPATIBLE,
    INCOMPATIBLE,

    /** Used internally to tag a reader/writer schema pair and prevent recursion. */
    RECURSION_IN_PROGRESS;
  }

  // -----------------------------------------------------------------------------------------------

  /** Borrowed from Guava's Objects.equal(a, b) */
  static boolean objectsEqual(Object obj1, Object obj2) {
    return (obj1 == obj2) || ((obj1 != null) && obj1.equals(obj2));
  }
}
