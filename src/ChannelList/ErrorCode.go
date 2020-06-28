package ChannelList

// error message list

const (
    INVALID_MESSAGE = "Invalid message received..."
    INVALID_CHANNEL = "No such channel found..."
    LOG_WRITE_FAILURE = "Failed to write logs, please check disk space and file rights..."
    PERSISTENT_CONFIG_ERROR = "Persistent needs file storage active, please check server config..."
    SAME_SUBSCRIBER_DETECTED = "Same subscriber already connected..."
    INVALID_PULL_FLAG = "Invalid start from flag, must be BEGINNING|NOPULL|LASTRECEIVED ..."
    INVALID_AGENT = "Invalid message type must be either publish or subscribe..."
    INVALID_SUBSCRIBER_OFFSET = "No subscriber offset file found, cannot start subscriber..."
    SUBSCRIBER_FULL = "Number of subscriber in a group cannot be greater than number of partitions"
)