/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.gms.claimclasses;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class ClaimStatus_ClaimStatusClaimLink extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -428628865529862834L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ClaimStatus_ClaimStatusClaimLink\",\"namespace\":\"org.gms.claimclasses\",\"fields\":[{\"name\":\"CS_Description\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"CL_ClaimID\",\"type\":\"int\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<ClaimStatus_ClaimStatusClaimLink> ENCODER =
      new BinaryMessageEncoder<ClaimStatus_ClaimStatusClaimLink>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<ClaimStatus_ClaimStatusClaimLink> DECODER =
      new BinaryMessageDecoder<ClaimStatus_ClaimStatusClaimLink>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<ClaimStatus_ClaimStatusClaimLink> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<ClaimStatus_ClaimStatusClaimLink> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<ClaimStatus_ClaimStatusClaimLink>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this ClaimStatus_ClaimStatusClaimLink to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a ClaimStatus_ClaimStatusClaimLink from a ByteBuffer. */
  public static ClaimStatus_ClaimStatusClaimLink fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public java.lang.CharSequence CS_Description;
  @Deprecated public int CL_ClaimID;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public ClaimStatus_ClaimStatusClaimLink() {}

  /**
   * All-args constructor.
   * @param CS_Description The new value for CS_Description
   * @param CL_ClaimID The new value for CL_ClaimID
   */
  public ClaimStatus_ClaimStatusClaimLink(java.lang.CharSequence CS_Description, java.lang.Integer CL_ClaimID) {
    this.CS_Description = CS_Description;
    this.CL_ClaimID = CL_ClaimID;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return CS_Description;
    case 1: return CL_ClaimID;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: CS_Description = (java.lang.CharSequence)value$; break;
    case 1: CL_ClaimID = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'CS_Description' field.
   * @return The value of the 'CS_Description' field.
   */
  public java.lang.CharSequence getCSDescription() {
    return CS_Description;
  }

  /**
   * Sets the value of the 'CS_Description' field.
   * @param value the value to set.
   */
  public void setCSDescription(java.lang.CharSequence value) {
    this.CS_Description = value;
  }

  /**
   * Gets the value of the 'CL_ClaimID' field.
   * @return The value of the 'CL_ClaimID' field.
   */
  public java.lang.Integer getCLClaimID() {
    return CL_ClaimID;
  }

  /**
   * Sets the value of the 'CL_ClaimID' field.
   * @param value the value to set.
   */
  public void setCLClaimID(java.lang.Integer value) {
    this.CL_ClaimID = value;
  }

  /**
   * Creates a new ClaimStatus_ClaimStatusClaimLink RecordBuilder.
   * @return A new ClaimStatus_ClaimStatusClaimLink RecordBuilder
   */
  public static org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder newBuilder() {
    return new org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder();
  }

  /**
   * Creates a new ClaimStatus_ClaimStatusClaimLink RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new ClaimStatus_ClaimStatusClaimLink RecordBuilder
   */
  public static org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder newBuilder(org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder other) {
    return new org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder(other);
  }

  /**
   * Creates a new ClaimStatus_ClaimStatusClaimLink RecordBuilder by copying an existing ClaimStatus_ClaimStatusClaimLink instance.
   * @param other The existing instance to copy.
   * @return A new ClaimStatus_ClaimStatusClaimLink RecordBuilder
   */
  public static org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder newBuilder(org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink other) {
    return new org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder(other);
  }

  /**
   * RecordBuilder for ClaimStatus_ClaimStatusClaimLink instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ClaimStatus_ClaimStatusClaimLink>
    implements org.apache.avro.data.RecordBuilder<ClaimStatus_ClaimStatusClaimLink> {

    private java.lang.CharSequence CS_Description;
    private int CL_ClaimID;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.CS_Description)) {
        this.CS_Description = data().deepCopy(fields()[0].schema(), other.CS_Description);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.CL_ClaimID)) {
        this.CL_ClaimID = data().deepCopy(fields()[1].schema(), other.CL_ClaimID);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing ClaimStatus_ClaimStatusClaimLink instance
     * @param other The existing instance to copy.
     */
    private Builder(org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.CS_Description)) {
        this.CS_Description = data().deepCopy(fields()[0].schema(), other.CS_Description);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.CL_ClaimID)) {
        this.CL_ClaimID = data().deepCopy(fields()[1].schema(), other.CL_ClaimID);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'CS_Description' field.
      * @return The value.
      */
    public java.lang.CharSequence getCSDescription() {
      return CS_Description;
    }

    /**
      * Sets the value of the 'CS_Description' field.
      * @param value The value of 'CS_Description'.
      * @return This builder.
      */
    public org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder setCSDescription(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.CS_Description = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'CS_Description' field has been set.
      * @return True if the 'CS_Description' field has been set, false otherwise.
      */
    public boolean hasCSDescription() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'CS_Description' field.
      * @return This builder.
      */
    public org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder clearCSDescription() {
      CS_Description = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'CL_ClaimID' field.
      * @return The value.
      */
    public java.lang.Integer getCLClaimID() {
      return CL_ClaimID;
    }

    /**
      * Sets the value of the 'CL_ClaimID' field.
      * @param value The value of 'CL_ClaimID'.
      * @return This builder.
      */
    public org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder setCLClaimID(int value) {
      validate(fields()[1], value);
      this.CL_ClaimID = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'CL_ClaimID' field has been set.
      * @return True if the 'CL_ClaimID' field has been set, false otherwise.
      */
    public boolean hasCLClaimID() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'CL_ClaimID' field.
      * @return This builder.
      */
    public org.gms.claimclasses.ClaimStatus_ClaimStatusClaimLink.Builder clearCLClaimID() {
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClaimStatus_ClaimStatusClaimLink build() {
      try {
        ClaimStatus_ClaimStatusClaimLink record = new ClaimStatus_ClaimStatusClaimLink();
        record.CS_Description = fieldSetFlags()[0] ? this.CS_Description : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.CL_ClaimID = fieldSetFlags()[1] ? this.CL_ClaimID : (java.lang.Integer) defaultValue(fields()[1]);
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<ClaimStatus_ClaimStatusClaimLink>
    WRITER$ = (org.apache.avro.io.DatumWriter<ClaimStatus_ClaimStatusClaimLink>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<ClaimStatus_ClaimStatusClaimLink>
    READER$ = (org.apache.avro.io.DatumReader<ClaimStatus_ClaimStatusClaimLink>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
