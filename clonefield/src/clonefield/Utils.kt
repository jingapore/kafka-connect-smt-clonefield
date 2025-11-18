package clonefield

import java.text.SimpleDateFormat
import java.time.ZoneId
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException

internal const val FROM_FIELDNAME = "from"
internal const val TO_FIELDNAME = "to"
internal const val CACHE_SIZE: Int = 16

// string is an ISO format that contains timezone. we cannot use Timestamp (https://kafka.apache.org/11/javadoc/org/apache/kafka/connect/data/Timestamp.html)
// because Timestamp doesn't have any representation for timezone.
internal const val TARGET_TYPE_STRING = "string";

// https://kafka.apache.org/11/javadoc/org/apache/kafka/connect/data/Date.html
internal const val TARGET_TYPE_DATE = "Date";

// https://kafka.apache.org/11/javadoc/org/apache/kafka/connect/data/Time.html
internal const val TARGET_TYPE_TIME = "Time";

internal val CONFIG_DEF: ConfigDef = ConfigDef().apply {
    define(
        FROM_FIELDNAME,
        ConfigDef.Type.STRING,
        ConfigDef.NO_DEFAULT_VALUE,
        ConfigDef.Importance.HIGH,
        "Name of the field that we want to clone"
    )
    define(
        TO_FIELDNAME,
        ConfigDef.Type.STRING,
        ConfigDef.NO_DEFAULT_VALUE,
        ConfigDef.Importance.HIGH,
        "Name of the new field to populate with cloned value"
    )
}

data class Config(
    val from: String,
    val to: String
)
