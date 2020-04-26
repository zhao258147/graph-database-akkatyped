CREATE DATABASE IF NOT EXISTS graph;

CREATE TABLE IF NOT EXISTS graph.nodes (
    id VARCHAR(36) PRIMARY KEY,
    nodetype VARCHAR(36),
    tags TEXT,
    properties TEXT
)

CREATE TABLE IF NOT EXISTS graph.clicks (
    id VARCHAR(36) PRIMARY KEY,
    nodetype VARCHAR(36),
    ts BIGINT,
    clicks INT
)

CREATE TABLE IF NOT EXISTS graph.read_side_offsets (
    tag VARCHAR(36) PRIMARY KEY,
    offset varchar(36)
)