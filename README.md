# octo
Fast, performant file uploader for Google Cloud Storage

## Quickstart

### Creating a service account
_Note: If you're running on an Google Compute Engine, you may skip this section._

First, [create a service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating) for use. The minimum required IAM roles are:
- `Logs Writer`
- `Pub/Sub Publisher`
- `Storage Object Admin`

Download the `.json` service account file and ensure the name of the file is exported as environment variable `GOOGLE_APPLICATION_CREDENTIALS` like this:
```sh
GOOGLE_APPLICATION_CREDENTIALS=<your-service-account-file>.json
```

### Build Go binary
Use the `go build` command to build:
```sh
go build .
```

Modify the permissions to make sure the binary is executable
```sh
chmod +x octo
```

### Install binary
Copy the binary executable to a common directory, like `/usr/local/bin` for example:
```sh
sudo cp octo /usr/local/bin/
```

### Execute binary
To execute the binary, you can simply run:
```sh
octo
```

List of command flags:
```sh
$ octo -h
Fast, performant file uploader for Google Cloud Storage
More information: https://github.com/loozhengyuan/octo

Usage:
  octo [command]

Available Commands:
  help        Help about any command
  up          Upload files matching a glob pattern

Flags:
  -h, --help   help for octo

Use "octo [command] --help" for more information about a command.
```
```sh
$ octo up -h
Upload files matching a glob pattern

Usage:
  octo up <glob pattern> [flags]

Examples:
  octo up '*.gz' -p my-project -b my-bucket -t my-topic

Flags:
  -b, --bucket string    name of the Storage bucket to upload
  -h, --help             help for up
      --prefix string    string prefix to append to the blob
  -p, --project string   name of the Google Cloud project
  -t, --topic string     name of the Pub/Sub topic to publish
      --workers int      number of workers nodes to spawn (default 10)
```

### Optional: Running as cron job
You can also run it as a cron job if you wish. First, create a log file:
```sh
sudo touch /var/log/octo.log
sudo chmod 666 /var/log/octo.log
```

Pipe the output of the file to the log file:
```sh
octo 2>&1 | tee -a /var/log/octo.log
```
_Note: `2>&1` pipes STDERR to STDOUT, while `tee -a` appends the output of STDOUT to `/var/log/octo.log`_
