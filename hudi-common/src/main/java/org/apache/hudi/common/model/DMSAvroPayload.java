package org.apache.hudi.common.model;

import org.apache.hudi.common.util.Option;
import org.apache.avro.generic.GenericRecord;

public class DMSAvroPayload extends OverwriteWithLatestAvroPayload {

    // Field is prefixed with a underscore by transformer to indicate metadata field
    public static final String OP_FIELD = "Op";
    public static final String DELETE_OP = "D";

    public DMSAvroPayload(GenericRecord record, Comparable orderingVal) {
        super(record, orderingVal);
    }

    public DMSAvroPayload(Option<GenericRecord> record) {
        this(record.isPresent() ? record.get() : null, 0); // natural order
    }

    @Override
    protected boolean isDeleteRecord(GenericRecord genericRecord) {
        return (genericRecord.get(OP_FIELD) != null && genericRecord.get(OP_FIELD).toString().equalsIgnoreCase(
                DELETE_OP));
    }
}
