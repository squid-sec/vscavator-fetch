# VSCavator Fetch

VSCavator Fetch enumerates every VSCode extension publisher and release on the marketplace storing the data in a Postgres database (extension, publisher, release, and review metadata) and S3 bucket (raw extension files). This script is the backbone of the VSCavator application.

### Code

##### Tests

To run unit tests: `python -m unittest {test_file}.py`
To run integration tests: TODO

### RFCs

[Initial Design](https://docs.google.com/document/d/17PYEKyeX7ISVeifeeQfQkeA7-YrqzbRZH4XXvHL-tvY/edit?usp=sharing)
