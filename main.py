import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
from apache_beam.pvalue import TaggedOutput
import apache_beam.io.fileio
import apache_beam as beam
import argparse
import io
import json
import zipfile
import logging
import re


class ExtractFilesFn(beam.DoFn):
    OUTPUT_TAG_METADATA = "tag_metadata"

    def __init__(self, extract_location: str, channels_re: str = None):
        self.extract_location = extract_location
        self.channels_re = channels_re

    def process(self, element: apache_beam.io.fileio.ReadableFile):
        with zipfile.ZipFile(
            io.BytesIO(element.read()), "r", metadata_encoding="utf-8"
        ) as zip_ref:
            for filename in zip_ref.namelist():
                if zip_ref.getinfo(filename).is_dir():
                    continue

                with zip_ref.open(filename) as f:
                    file_path = FileSystems.join(self.extract_location, filename)
                    dir, base = os.path.split(filename)
                    if self.channels_re:
                        if not re.match(self.channels_re, dir):
                            continue
                    with FileSystems.create(file_path) as out:
                        out.write(f.read())
                    if re.match(r"\d{4}-\d{2}-\d{2}\.json", base):
                        yield file_path
                    else:
                        yield TaggedOutput(
                            ExtractFilesFn.OUTPUT_TAG_METADATA, file_path
                        )


class ReadAndFormatMessagesFn(beam.DoFn):
    BIGQUERY_SCHEMA = {
        "fields": [
            {"name": "ts", "type": "STRING", "mode": "REQUIRED"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "subtype", "type": "STRING", "mode": "NULLABLE"},
            {"name": "channel", "type": "STRING", "mode": "REQUIRED"},
            {"name": "user_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "text", "type": "STRING", "mode": "REQUIRED"},
            {"name": "reactions", "type": "STRING", "mode": "NULLABLE"},
        ]
    }

    def process(self, filename: str):
        # path/to/channel/YYYY-MM-DD.json -> channel
        channel = FileSystems.split(filename)[0].split("/")[-1]
        with FileSystems.open(filename) as f:
            messages = json.loads(f.read())
            for message in messages:
                try:
                    yield [
                        {
                            "ts": message["ts"],
                            "timestamp": float(message["ts"]),
                            "subtype": message.get("subtype"),
                            "channel": channel,
                            "user_name": message.get("user_profile", {}).get("name"),
                            "text": message.get("text"),
                            "reactions": json.dumps(message["reactions"])
                            if "reactions" in message
                            else None,
                        }
                    ]
                except KeyError as e:
                    logging.warn("%s: KeyError %s: %s", filename, e, message)


class ReadAndFormatUsersFn(beam.DoFn):
    BIGQUERY_SCHEMA = {
        "fields": [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "json", "type": "STRING", "mode": "REQUIRED"},
        ]
    }

    def process(self, filename: str):
        with FileSystems.open(filename) as f:
            users = json.loads(f.read())
            return [
                {
                    "id": user["id"],
                    "json": json.dumps(user),
                }
                for user in users
            ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", type=str, default="./*.zip", help="Input file pattern"
    )
    parser.add_argument("--output_dataset", type=str, help="Output BigQuery dataset")
    parser.add_argument(
        "--extract_location",
        default="./tmp",
        type=str,
        help="Location to extract files",
    )
    parser.add_argument(
        "--channels_re",
        type=str,
        help="Regex for channels to extract",
    )

    args, beam_args = parser.parse_known_args()
    beam_options = PipelineOptions(beam_args)
    project = beam_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions
    ).project
    if project and not os.environ.get("GOOGLE_CLOUD_PROJECT"):
        os.environ["GOOGLE_CLOUD_PROJECT"] = project

    with beam.Pipeline(options=beam_options) as pipeline:
        messages, meta = (
            pipeline
            | "Match files" >> beam.io.fileio.MatchFiles(args.input)
            | "Read matches" >> beam.io.fileio.ReadMatches()
            | "Extract files"
            >> beam.ParDo(
                ExtractFilesFn(args.extract_location, args.channels_re)
            ).with_outputs(
                ExtractFilesFn.OUTPUT_TAG_METADATA,
                main="_messages",
            )
        )

        messages = messages | "Read and Format Messages" >> beam.ParDo(
            ReadAndFormatMessagesFn()
        )
        users = (
            meta
            | beam.Filter(lambda file: file.endswith("users.json"))
            | "Read and Format Users" >> beam.ParDo(ReadAndFormatUsersFn())
        )

        if project and args.output_dataset:
            messages | "Write Messages to BigQuery" >> beam.io.WriteToBigQuery(
                project=project,
                table=args.output_dataset + ".messages",
                schema=ReadAndFormatMessagesFn.BIGQUERY_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
            users | "Write Users to BigQuery" >> beam.io.WriteToBigQuery(
                project=project,
                table=args.output_dataset + ".users",
                schema=ReadAndFormatUsersFn.BIGQUERY_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        else:
            messages | "Debug Message" >> beam.Map(lambda msg: logging.info(msg))
            users | "Debug Users" >> beam.Map(
                lambda msg: logging.info("users: %s", msg)
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
