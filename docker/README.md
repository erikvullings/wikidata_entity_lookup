# Creating the docker image

Creates a lightweight, self-contained Docker image ready to serve your Wikidata entities via KeyDB.

## How It Works

### Stage 1 (Builder Stage):

- The KeyDB service starts temporarily, and a Python script loads the key-value pairs from the `kv_data.msgpack` file into KeyDB.
- KeyDB's SAVE command is used to generate a `dump.rdb` file that represents the database state.
- The builder stage ensures that the `dump.rdb` file is created and ready to be copied to the final image.

### Stage 2 (Final Stage):

- Copies the `dump.rdb` file from the builder stage into the final image.
- The final image runs KeyDB with the `dump.rdb` preloaded, so the database starts with your data already in memory.

## Building and Running the Docker Image

1. Build the image:
  ```bash
  Copy code
  docker build -t wikidata-keydb .
  ```
2. Run the container:
  ```bash
  Copy code
  docker run -p 6379:6379 wikidata-keydb
  ```
3. Verify the preloaded data:
  ```bash
  Copy code
  redis-cli
  > KEYS *
  ```