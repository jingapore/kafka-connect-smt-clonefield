package clonefield

import java.text.SimpleDateFormat
import java.time.ZoneId
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException

internal const val FROM_FIELDNAME = "from"
internal const val TO_FIELDNAME = "to"
internal const val CACHE_SIZE: Int = 16

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
