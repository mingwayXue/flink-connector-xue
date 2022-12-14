// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: test/test.proto

package com.xue.bigdata.test.protobuf;

/**
 * Protobuf type {@code Test}
 */
public final class Test extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:Test)
    TestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use Test.newBuilder() to construct.
  private Test(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private Test() {
    name_ = "";
    names_ = com.google.protobuf.LazyStringArrayList.EMPTY;
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new Test();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private Test(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 10: {
            java.lang.String s = input.readStringRequireUtf8();

            name_ = s;
            break;
          }
          case 18: {
            java.lang.String s = input.readStringRequireUtf8();
            if (!((mutable_bitField0_ & 0x00000001) != 0)) {
              names_ = new com.google.protobuf.LazyStringArrayList();
              mutable_bitField0_ |= 0x00000001;
            }
            names_.add(s);
            break;
          }
          case 58: {
            if (!((mutable_bitField0_ & 0x00000002) != 0)) {
              siMap_ = com.google.protobuf.MapField.newMapField(
                  SiMapDefaultEntryHolder.defaultEntry);
              mutable_bitField0_ |= 0x00000002;
            }
            com.google.protobuf.MapEntry<java.lang.String, java.lang.Integer>
            siMap__ = input.readMessage(
                SiMapDefaultEntryHolder.defaultEntry.getParserForType(), extensionRegistry);
            siMap_.getMutableMap().put(
                siMap__.getKey(), siMap__.getValue());
            break;
          }
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      if (((mutable_bitField0_ & 0x00000001) != 0)) {
        names_ = names_.getUnmodifiableView();
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.xue.bigdata.test.protobuf.TestOuterClassname.internal_static_Test_descriptor;
  }

  @SuppressWarnings({"rawtypes"})
  @java.lang.Override
  protected com.google.protobuf.MapField internalGetMapField(
      int number) {
    switch (number) {
      case 7:
        return internalGetSiMap();
      default:
        throw new RuntimeException(
            "Invalid map field number: " + number);
    }
  }
  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.xue.bigdata.test.protobuf.TestOuterClassname.internal_static_Test_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.xue.bigdata.test.protobuf.Test.class, com.xue.bigdata.test.protobuf.Test.Builder.class);
  }

  public static final int NAME_FIELD_NUMBER = 1;
  private volatile java.lang.Object name_;
  /**
   * <code>string name = 1;</code>
   * @return The name.
   */
  @java.lang.Override
  public java.lang.String getName() {
    java.lang.Object ref = name_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      name_ = s;
      return s;
    }
  }
  /**
   * <code>string name = 1;</code>
   * @return The bytes for name.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getNameBytes() {
    java.lang.Object ref = name_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      name_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int NAMES_FIELD_NUMBER = 2;
  private com.google.protobuf.LazyStringList names_;
  /**
   * <code>repeated string names = 2;</code>
   * @return A list containing the names.
   */
  public com.google.protobuf.ProtocolStringList
      getNamesList() {
    return names_;
  }
  /**
   * <code>repeated string names = 2;</code>
   * @return The count of names.
   */
  public int getNamesCount() {
    return names_.size();
  }
  /**
   * <code>repeated string names = 2;</code>
   * @param index The index of the element to return.
   * @return The names at the given index.
   */
  public java.lang.String getNames(int index) {
    return names_.get(index);
  }
  /**
   * <code>repeated string names = 2;</code>
   * @param index The index of the value to return.
   * @return The bytes of the names at the given index.
   */
  public com.google.protobuf.ByteString
      getNamesBytes(int index) {
    return names_.getByteString(index);
  }

  public static final int SI_MAP_FIELD_NUMBER = 7;
  private static final class SiMapDefaultEntryHolder {
    static final com.google.protobuf.MapEntry<
        java.lang.String, java.lang.Integer> defaultEntry =
            com.google.protobuf.MapEntry
            .<java.lang.String, java.lang.Integer>newDefaultInstance(
                com.xue.bigdata.test.protobuf.TestOuterClassname.internal_static_Test_SiMapEntry_descriptor, 
                com.google.protobuf.WireFormat.FieldType.STRING,
                "",
                com.google.protobuf.WireFormat.FieldType.INT32,
                0);
  }
  private com.google.protobuf.MapField<
      java.lang.String, java.lang.Integer> siMap_;
  private com.google.protobuf.MapField<java.lang.String, java.lang.Integer>
  internalGetSiMap() {
    if (siMap_ == null) {
      return com.google.protobuf.MapField.emptyMapField(
          SiMapDefaultEntryHolder.defaultEntry);
    }
    return siMap_;
  }

  public int getSiMapCount() {
    return internalGetSiMap().getMap().size();
  }
  /**
   * <code>map&lt;string, int32&gt; si_map = 7;</code>
   */

  @java.lang.Override
  public boolean containsSiMap(
      java.lang.String key) {
    if (key == null) { throw new NullPointerException("map key"); }
    return internalGetSiMap().getMap().containsKey(key);
  }
  /**
   * Use {@link #getSiMapMap()} instead.
   */
  @java.lang.Override
  @java.lang.Deprecated
  public java.util.Map<java.lang.String, java.lang.Integer> getSiMap() {
    return getSiMapMap();
  }
  /**
   * <code>map&lt;string, int32&gt; si_map = 7;</code>
   */
  @java.lang.Override

  public java.util.Map<java.lang.String, java.lang.Integer> getSiMapMap() {
    return internalGetSiMap().getMap();
  }
  /**
   * <code>map&lt;string, int32&gt; si_map = 7;</code>
   */
  @java.lang.Override

  public int getSiMapOrDefault(
      java.lang.String key,
      int defaultValue) {
    if (key == null) { throw new NullPointerException("map key"); }
    java.util.Map<java.lang.String, java.lang.Integer> map =
        internalGetSiMap().getMap();
    return map.containsKey(key) ? map.get(key) : defaultValue;
  }
  /**
   * <code>map&lt;string, int32&gt; si_map = 7;</code>
   */
  @java.lang.Override

  public int getSiMapOrThrow(
      java.lang.String key) {
    if (key == null) { throw new NullPointerException("map key"); }
    java.util.Map<java.lang.String, java.lang.Integer> map =
        internalGetSiMap().getMap();
    if (!map.containsKey(key)) {
      throw new java.lang.IllegalArgumentException();
    }
    return map.get(key);
  }

  private byte memoizedIsInitialized = -1;
  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, name_);
    }
    for (int i = 0; i < names_.size(); i++) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, names_.getRaw(i));
    }
    com.google.protobuf.GeneratedMessageV3
      .serializeStringMapTo(
        output,
        internalGetSiMap(),
        SiMapDefaultEntryHolder.defaultEntry,
        7);
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, name_);
    }
    {
      int dataSize = 0;
      for (int i = 0; i < names_.size(); i++) {
        dataSize += computeStringSizeNoTag(names_.getRaw(i));
      }
      size += dataSize;
      size += 1 * getNamesList().size();
    }
    for (java.util.Map.Entry<java.lang.String, java.lang.Integer> entry
         : internalGetSiMap().getMap().entrySet()) {
      com.google.protobuf.MapEntry<java.lang.String, java.lang.Integer>
      siMap__ = SiMapDefaultEntryHolder.defaultEntry.newBuilderForType()
          .setKey(entry.getKey())
          .setValue(entry.getValue())
          .build();
      size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(7, siMap__);
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof com.xue.bigdata.test.protobuf.Test)) {
      return super.equals(obj);
    }
    com.xue.bigdata.test.protobuf.Test other = (com.xue.bigdata.test.protobuf.Test) obj;

    if (!getName()
        .equals(other.getName())) return false;
    if (!getNamesList()
        .equals(other.getNamesList())) return false;
    if (!internalGetSiMap().equals(
        other.internalGetSiMap())) return false;
    if (!unknownFields.equals(other.unknownFields)) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    hash = (37 * hash) + NAME_FIELD_NUMBER;
    hash = (53 * hash) + getName().hashCode();
    if (getNamesCount() > 0) {
      hash = (37 * hash) + NAMES_FIELD_NUMBER;
      hash = (53 * hash) + getNamesList().hashCode();
    }
    if (!internalGetSiMap().getMap().isEmpty()) {
      hash = (37 * hash) + SI_MAP_FIELD_NUMBER;
      hash = (53 * hash) + internalGetSiMap().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static com.xue.bigdata.test.protobuf.Test parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.xue.bigdata.test.protobuf.Test parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.xue.bigdata.test.protobuf.Test parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.xue.bigdata.test.protobuf.Test parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.xue.bigdata.test.protobuf.Test parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.xue.bigdata.test.protobuf.Test parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.xue.bigdata.test.protobuf.Test parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.xue.bigdata.test.protobuf.Test parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.xue.bigdata.test.protobuf.Test parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static com.xue.bigdata.test.protobuf.Test parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.xue.bigdata.test.protobuf.Test parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.xue.bigdata.test.protobuf.Test parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @java.lang.Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(com.xue.bigdata.test.protobuf.Test prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @java.lang.Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code Test}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:Test)
      com.xue.bigdata.test.protobuf.TestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.xue.bigdata.test.protobuf.TestOuterClassname.internal_static_Test_descriptor;
    }

    @SuppressWarnings({"rawtypes"})
    protected com.google.protobuf.MapField internalGetMapField(
        int number) {
      switch (number) {
        case 7:
          return internalGetSiMap();
        default:
          throw new RuntimeException(
              "Invalid map field number: " + number);
      }
    }
    @SuppressWarnings({"rawtypes"})
    protected com.google.protobuf.MapField internalGetMutableMapField(
        int number) {
      switch (number) {
        case 7:
          return internalGetMutableSiMap();
        default:
          throw new RuntimeException(
              "Invalid map field number: " + number);
      }
    }
    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.xue.bigdata.test.protobuf.TestOuterClassname.internal_static_Test_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.xue.bigdata.test.protobuf.Test.class, com.xue.bigdata.test.protobuf.Test.Builder.class);
    }

    // Construct using com.xue.bigdata.test.protobuf.Test.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      name_ = "";

      names_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      internalGetMutableSiMap().clear();
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.xue.bigdata.test.protobuf.TestOuterClassname.internal_static_Test_descriptor;
    }

    @java.lang.Override
    public com.xue.bigdata.test.protobuf.Test getDefaultInstanceForType() {
      return com.xue.bigdata.test.protobuf.Test.getDefaultInstance();
    }

    @java.lang.Override
    public com.xue.bigdata.test.protobuf.Test build() {
      com.xue.bigdata.test.protobuf.Test result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public com.xue.bigdata.test.protobuf.Test buildPartial() {
      com.xue.bigdata.test.protobuf.Test result = new com.xue.bigdata.test.protobuf.Test(this);
      int from_bitField0_ = bitField0_;
      result.name_ = name_;
      if (((bitField0_ & 0x00000001) != 0)) {
        names_ = names_.getUnmodifiableView();
        bitField0_ = (bitField0_ & ~0x00000001);
      }
      result.names_ = names_;
      result.siMap_ = internalGetSiMap();
      result.siMap_.makeImmutable();
      onBuilt();
      return result;
    }

    @java.lang.Override
    public Builder clone() {
      return super.clone();
    }
    @java.lang.Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.setField(field, value);
    }
    @java.lang.Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @java.lang.Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @java.lang.Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @java.lang.Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.addRepeatedField(field, value);
    }
    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.xue.bigdata.test.protobuf.Test) {
        return mergeFrom((com.xue.bigdata.test.protobuf.Test)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.xue.bigdata.test.protobuf.Test other) {
      if (other == com.xue.bigdata.test.protobuf.Test.getDefaultInstance()) return this;
      if (!other.getName().isEmpty()) {
        name_ = other.name_;
        onChanged();
      }
      if (!other.names_.isEmpty()) {
        if (names_.isEmpty()) {
          names_ = other.names_;
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          ensureNamesIsMutable();
          names_.addAll(other.names_);
        }
        onChanged();
      }
      internalGetMutableSiMap().mergeFrom(
          other.internalGetSiMap());
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      com.xue.bigdata.test.protobuf.Test parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (com.xue.bigdata.test.protobuf.Test) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object name_ = "";
    /**
     * <code>string name = 1;</code>
     * @return The name.
     */
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        name_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string name = 1;</code>
     * @return The bytes for name.
     */
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string name = 1;</code>
     * @param value The name to set.
     * @return This builder for chaining.
     */
    public Builder setName(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      name_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string name = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearName() {
      
      name_ = getDefaultInstance().getName();
      onChanged();
      return this;
    }
    /**
     * <code>string name = 1;</code>
     * @param value The bytes for name to set.
     * @return This builder for chaining.
     */
    public Builder setNameBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      name_ = value;
      onChanged();
      return this;
    }

    private com.google.protobuf.LazyStringList names_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    private void ensureNamesIsMutable() {
      if (!((bitField0_ & 0x00000001) != 0)) {
        names_ = new com.google.protobuf.LazyStringArrayList(names_);
        bitField0_ |= 0x00000001;
       }
    }
    /**
     * <code>repeated string names = 2;</code>
     * @return A list containing the names.
     */
    public com.google.protobuf.ProtocolStringList
        getNamesList() {
      return names_.getUnmodifiableView();
    }
    /**
     * <code>repeated string names = 2;</code>
     * @return The count of names.
     */
    public int getNamesCount() {
      return names_.size();
    }
    /**
     * <code>repeated string names = 2;</code>
     * @param index The index of the element to return.
     * @return The names at the given index.
     */
    public java.lang.String getNames(int index) {
      return names_.get(index);
    }
    /**
     * <code>repeated string names = 2;</code>
     * @param index The index of the value to return.
     * @return The bytes of the names at the given index.
     */
    public com.google.protobuf.ByteString
        getNamesBytes(int index) {
      return names_.getByteString(index);
    }
    /**
     * <code>repeated string names = 2;</code>
     * @param index The index to set the value at.
     * @param value The names to set.
     * @return This builder for chaining.
     */
    public Builder setNames(
        int index, java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureNamesIsMutable();
      names_.set(index, value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string names = 2;</code>
     * @param value The names to add.
     * @return This builder for chaining.
     */
    public Builder addNames(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureNamesIsMutable();
      names_.add(value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string names = 2;</code>
     * @param values The names to add.
     * @return This builder for chaining.
     */
    public Builder addAllNames(
        java.lang.Iterable<java.lang.String> values) {
      ensureNamesIsMutable();
      com.google.protobuf.AbstractMessageLite.Builder.addAll(
          values, names_);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string names = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearNames() {
      names_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string names = 2;</code>
     * @param value The bytes of the names to add.
     * @return This builder for chaining.
     */
    public Builder addNamesBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      ensureNamesIsMutable();
      names_.add(value);
      onChanged();
      return this;
    }

    private com.google.protobuf.MapField<
        java.lang.String, java.lang.Integer> siMap_;
    private com.google.protobuf.MapField<java.lang.String, java.lang.Integer>
    internalGetSiMap() {
      if (siMap_ == null) {
        return com.google.protobuf.MapField.emptyMapField(
            SiMapDefaultEntryHolder.defaultEntry);
      }
      return siMap_;
    }
    private com.google.protobuf.MapField<java.lang.String, java.lang.Integer>
    internalGetMutableSiMap() {
      onChanged();;
      if (siMap_ == null) {
        siMap_ = com.google.protobuf.MapField.newMapField(
            SiMapDefaultEntryHolder.defaultEntry);
      }
      if (!siMap_.isMutable()) {
        siMap_ = siMap_.copy();
      }
      return siMap_;
    }

    public int getSiMapCount() {
      return internalGetSiMap().getMap().size();
    }
    /**
     * <code>map&lt;string, int32&gt; si_map = 7;</code>
     */

    @java.lang.Override
    public boolean containsSiMap(
        java.lang.String key) {
      if (key == null) { throw new NullPointerException("map key"); }
      return internalGetSiMap().getMap().containsKey(key);
    }
    /**
     * Use {@link #getSiMapMap()} instead.
     */
    @java.lang.Override
    @java.lang.Deprecated
    public java.util.Map<java.lang.String, java.lang.Integer> getSiMap() {
      return getSiMapMap();
    }
    /**
     * <code>map&lt;string, int32&gt; si_map = 7;</code>
     */
    @java.lang.Override

    public java.util.Map<java.lang.String, java.lang.Integer> getSiMapMap() {
      return internalGetSiMap().getMap();
    }
    /**
     * <code>map&lt;string, int32&gt; si_map = 7;</code>
     */
    @java.lang.Override

    public int getSiMapOrDefault(
        java.lang.String key,
        int defaultValue) {
      if (key == null) { throw new NullPointerException("map key"); }
      java.util.Map<java.lang.String, java.lang.Integer> map =
          internalGetSiMap().getMap();
      return map.containsKey(key) ? map.get(key) : defaultValue;
    }
    /**
     * <code>map&lt;string, int32&gt; si_map = 7;</code>
     */
    @java.lang.Override

    public int getSiMapOrThrow(
        java.lang.String key) {
      if (key == null) { throw new NullPointerException("map key"); }
      java.util.Map<java.lang.String, java.lang.Integer> map =
          internalGetSiMap().getMap();
      if (!map.containsKey(key)) {
        throw new java.lang.IllegalArgumentException();
      }
      return map.get(key);
    }

    public Builder clearSiMap() {
      internalGetMutableSiMap().getMutableMap()
          .clear();
      return this;
    }
    /**
     * <code>map&lt;string, int32&gt; si_map = 7;</code>
     */

    public Builder removeSiMap(
        java.lang.String key) {
      if (key == null) { throw new NullPointerException("map key"); }
      internalGetMutableSiMap().getMutableMap()
          .remove(key);
      return this;
    }
    /**
     * Use alternate mutation accessors instead.
     */
    @java.lang.Deprecated
    public java.util.Map<java.lang.String, java.lang.Integer>
    getMutableSiMap() {
      return internalGetMutableSiMap().getMutableMap();
    }
    /**
     * <code>map&lt;string, int32&gt; si_map = 7;</code>
     */
    public Builder putSiMap(
        java.lang.String key,
        int value) {
      if (key == null) { throw new NullPointerException("map key"); }
      
      internalGetMutableSiMap().getMutableMap()
          .put(key, value);
      return this;
    }
    /**
     * <code>map&lt;string, int32&gt; si_map = 7;</code>
     */

    public Builder putAllSiMap(
        java.util.Map<java.lang.String, java.lang.Integer> values) {
      internalGetMutableSiMap().getMutableMap()
          .putAll(values);
      return this;
    }
    @java.lang.Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @java.lang.Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:Test)
  }

  // @@protoc_insertion_point(class_scope:Test)
  private static final com.xue.bigdata.test.protobuf.Test DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.xue.bigdata.test.protobuf.Test();
  }

  public static com.xue.bigdata.test.protobuf.Test getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<Test>
      PARSER = new com.google.protobuf.AbstractParser<Test>() {
    @java.lang.Override
    public Test parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new Test(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<Test> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<Test> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public com.xue.bigdata.test.protobuf.Test getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

