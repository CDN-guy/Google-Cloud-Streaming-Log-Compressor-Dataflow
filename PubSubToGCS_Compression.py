import argparse
from datetime import datetime
import logging
import random
import gzip  # Import the gzip library for compression
import zlib  # Import the zlib library for deflate compression
import bz2  # Import the bz2 library for bzip2 compression


from apache_beam import (
    DoFn,
    GroupByKey,
    io,
    ParDo,
    Pipeline,
    PTransform,
    WindowInto,
    WithKeys,
)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """

    def __init__(self, window_size, num_shards=5):
        # Set window size to the specified number of seconds.
        self.window_size = int(window_size)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            | "Decode message" >> ParDo(DecodeMessage())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )


class DecodeMessage(DoFn):
    def process(self, element):
        """Processes each element by decoding the message body."""
        yield element.decode("utf-8")


class WriteToGCS(DoFn):
    def __init__(self, output_gcs_directory, compression_method):
        self.output_gcs_directory = output_gcs_directory
        self.compression_method = compression_method

    def process(self, key_value, window=DoFn.WindowParam):
        """Write messages in a batch to Google Cloud Storage with the specified compression."""

        # Get the window start time and format it for the date-based directory
        window_start_dt = window.start.to_utc_datetime()
        date_dir = window_start_dt.strftime("%Y/%m/%d")

        # Format the window start and end times for the filename
        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = key_value

        # Construct the filename with the dynamic date directory
        suffix = "json"
        filename = f"{self.output_gcs_directory}/{date_dir}/logs-{window_start}-{window_end}-{str(shard_id)}.{suffix}"
        mode = "wb"
        mime_type = "application/octet-stream"  # Default MIME type

        if self.compression_method == "gzip":
            filename += ".gz"
            mime_type = "application/gzip"
        elif self.compression_method == "deflate":
            filename += ".zlib"
            mime_type = "application/zlib"
        elif self.compression_method == "bz2":
            filename += ".bz2"
            mime_type = "application/x-bzip2"
        else: # uncompressed
            mime_type = "application/json"

        # Use the appropriate compression method
        with io.gcsio.GcsIO().open(filename=filename, mode=mode, mime_type=mime_type) as f:
            if self.compression_method == "gzip":
                with gzip.GzipFile(fileobj=f, mode="wb") as gz_f:
                    for message_body in batch:
                        gz_f.write(f"{message_body}\n".encode())
            elif self.compression_method == "deflate":
                # zlib.compress works on a single byte string, so we join all messages first
                all_messages = b"".join(
                    [f"{message_body}\n".encode() for message_body in batch]
                )
                compressed_data = zlib.compress(all_messages)
                f.write(compressed_data)
            elif self.compression_method == "bz2":
                all_messages = b"".join(
                    [f"{message_body}\n".encode() for message_body in batch]
                )
                compressed_data = bz2.compress(all_messages)
                f.write(compressed_data)
            else: # uncompressed
                for message_body in batch:
                    f.write(f"{message_body}\n".encode())


def run(
    input_topic,
    output_gcs_directory,
    compression_type="gzip",
    window_interval_sec=60, # Default changed to 60 seconds
    num_shards=5,
    pipeline_args=None,
):

    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> io.ReadFromPubSub(topic=input_topic)
            | "Window into" >> GroupMessagesByFixedWindows(window_interval_sec, num_shards)
            | "Write to GCS" >> ParDo(WriteToGCS(output_gcs_directory, compression_type))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
    )
    parser.add_argument(
        "--compression_type",
        choices=["gzip", "deflate", "uncompressed", "bz2"],
        default="gzip",
        help=(
            "The compression algorithm to use for output files. Supported values:\n"
            "  'deflate': Deflate.\n"
            "  'bz2': bzip2.\n"
            "  'gzip': GZipped.\n"
            "  'uncompressed': Uncompressed (raw JSON)."
        )
    )
    parser.add_argument(
        "--window_interval_sec",
        type=int, # Changed type to int since it's now seconds
        default=60, # Default value changed to 60 seconds
        help="Output file's window size in seconds.", # Updated help text
    )
    parser.add_argument(
        "--output_gcs_directory",
        help="Directory path of the output GCS file."
        '"gs://<BUCKET_NAME>/<DIR>" (no trailing slash).',
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=5,
        help="Number of shards to use when writing windowed elements to GCS.",
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_topic,
        known_args.output_gcs_directory,
        known_args.compression_type,
        known_args.window_interval_sec,
        known_args.num_shards,
        pipeline_args,
    )
