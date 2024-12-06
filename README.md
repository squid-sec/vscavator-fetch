# VSCavator

VSCavator is the VSCode extension version of CRXcavator which scans web browser extensions. It enumerates every VSCode extension on the marketplace and then scans each of them based on various criteria. The goal is to understand the risk associated with installing and using each extension.

### Architecture

*Take it slow*
1) Fetch all VSCode extension metadata and store in tables or update if already retrieved (extensions and publishers)
2) Fetch all extension release information and store in different table (releases)
3) Fetch all extension .vsix files if not already retrieved and store in S3 (extensions) updating release table to point to the corresponding file


The first step is to fetch all VSCode extension data from the marketplace. This metadata will be collected every 4 hours and will be stored in a NoSQL database for easy deployment. The next step is to download each extension and store the zipped file in an S3 bucket. Note that there is both the possibility that an extension was deleted as well as a new extension was added between the two above actions causing the analysis to not be 100% complete. Since the data gets updated on a regularly scheduled cadence, this is not a concern. After the files are stored, they can be unzipped for analysis. Given that we are downloading untrusted files and code, we must isolate the workloads from the rest of the running processes so that in the case of malicious code being executed, the other systems are safe. The code inspection of extensions will include checking many things. The list of checks has yet to be determined, but overall it will include looking for whether or not the extension is accessing the internet, filesystem, or otherwise tampering with the user's computer. The result of this analysis will be inserted into a database which will serve as the source of truth for the trustworthiness of VSCode extensions.

Run `npm i --package-lock-only` and then `npm audit` to get a observability of package vulnerabilities.

### Security Controls

- Publisher is verified (note: this is not an effective control as anyone with a domain can become "verified")
- Extension is at last 90 days old (this gives the community time to potentially discover malicious activity)
- More to come...
