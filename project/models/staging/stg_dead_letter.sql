SELECT 
  source,
   record:batch_id::STRING AS batch_id,
   record:error_reason::STRING AS error_reason,
   record:kafka_key::STRING  AS kafka_key,
   record:kafka_offset::NUMBER AS kafka_offset,
   record:kafka_partition::NUMBER AS kafka_partition,
   record:rejected_at::TIMESTAMP_NTZ AS rejected_at,
   record as record_raw,
   _file_name,
   _loaded_at
    FROM  {{ source('raw', 'DEAD_LETTER_BRONZE') }}
