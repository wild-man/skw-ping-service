-- Initial schema for skw-srv1-service.

CREATE TABLE ping (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            TEXT,
);
