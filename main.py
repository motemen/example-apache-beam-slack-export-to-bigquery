from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
import apache_beam.io.fileio
import apache_beam as beam
import argparse
import io
import json
import zipfile
import logging


class ExtractFilesFn(beam.DoFn):
    def __init__(self, extract_location: str):
        self.extract_location = extract_location

    def process(self, element: apache_beam.io.fileio.ReadableFile):
        with zipfile.ZipFile(io.BytesIO(element.read()), "r") as zip_ref:
            for filename in zip_ref.namelist():
                if zip_ref.getinfo(filename).is_dir():
                    continue

                if not "/" in filename:
                    continue

                with zip_ref.open(filename) as f:
                    file_path = FileSystems.join(self.extract_location, filename)
                    with FileSystems.create(file_path) as out:
                        out.write(f.read())
                    yield file_path


class FormatJsonFn(beam.DoFn):
    def process(self, element: str):
        # path/to/channel/YYYY-MM-DD.json -> channel
        channel = FileSystems.split(element)[0].split("/")[-1]
        with FileSystems.open(element) as f:
            messages = json.loads(f.read())
            return [
                {
                    "ts": message["ts"],
                    "timestamp": float(message["ts"]),
                    "subtype": message.get("subtype"),
                    "channel": channel,
                    "user_name": message.get("user_profile", {}).get("name"),
                    "text": message.get("text"),
                    "reactions": json.dumps(message.get("reactions")),
                }
                for message in messages
            ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", type=str, default="./*.zip", help="Input file pattern"
    )
    parser.add_argument("--output_table", type=str, help="Output BigQuery table")
    parser.add_argument(
        "--extract_location",
        default="./tmp",
        type=str,
        help="Location to extract files",
    )

    args, beam_args = parser.parse_known_args()
    beam_options = PipelineOptions(beam_args)

    with beam.Pipeline(options=beam_options) as pipeline:
        messages = (
            pipeline
            | "Match files" >> beam.io.fileio.MatchFiles(args.input)
            | "Read matches" >> beam.io.fileio.ReadMatches()
            | "Extract files" >> beam.ParDo(ExtractFilesFn(args.extract_location))
            | "Format JSON" >> beam.ParDo(FormatJsonFn())
        )

        if args.output_table:
            messages | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=args.output_table,
                schema={
                    "fields": [
                        {"name": "ts", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                        {"name": "subtype", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "channel", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "user_name", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "text", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "reactions", "type": "STRING", "mode": "NULLABLE"},
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        else:
            messages | "Debug" >> beam.Map(lambda msg: logging.info(msg))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
