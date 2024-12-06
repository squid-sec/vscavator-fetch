import requests
import os
import zipfile

EXTENSIONS_URL = "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery"
DOWNLOAD_URL = "https://marketplace.visualstudio.com/_apis/public/gallery/publishers/{publisher}/vsextensions/{name}/{version}/vspackage"
HEADERS = {
    "Content-Type": "application/json",
    'accept': 'application/json;api-version=7.2-preview.1;excludeUrls=true',
}

def get_extensions():
    extensions = []
    for i in range(1, 3):
        payload = {
            'filters': [
                {
                    'criteria': [
                        {
                            'filterType': 8,
                            'value': 'Microsoft.VisualStudio.Code',
                        },
                        {
                            'filterType': 10,
                            'value': 'target:"Microsoft.VisualStudio.Code" ',
                        },
                    ],
                    'pageSize': 2,
                    'pageNumber': i,
                    'sortBy': 0,
                    'sortOrder': 0,
                },
            ],
        }

        response = requests.post(EXTENSIONS_URL, headers=HEADERS, json=payload)
        try:
            results = response.json()["results"][0]["extensions"]
            extensions.extend(results)
        except Exception as e:
            print(f"error while fetching extensions during iteration {str(i)}: {str(e)}")

    return extensions

def deduplicate_extensions(extensions):
    seen = set()
    deduplicated = []
    for extension in extensions:
        ext_id = extension.get("extensionId")
        if ext_id not in seen:
            seen.add(ext_id)
            deduplicated.append(extension)
    return deduplicated

def download_extension(publisher, name, version):
    url = DOWNLOAD_URL.format(publisher=publisher, name=name, version=version)

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        file_path = os.path.join("/Users/nwernink/Desktop/Coding/squid/vscavator/extensions/zipped", f"{publisher}-{name}-{version}.vsix")
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded: {file_path}")
    else:
        print(f"Failed to download {publisher}/{name}@{version}")

def unzip_file(file, extension_id):
    extract_to_folder = f"/Users/nwernink/Desktop/Coding/squid/vscavator/extensions/unzipped/{extension_id}"
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)

def get_extension_releases(identifier):
    json_data = {
        'assetTypes': None,
        'filters': [
            {
                'criteria': [
                    {
                        'filterType': 7,
                        'value': identifier,
                    },
                ],
                'direction': 2,
                'pageSize': 100,
                'pageNumber': 1,
                'sortBy': 0,
                'sortOrder': 0,
                'pagingToken': None,
            },
        ],
        'flags': 2151,
    }

    response = requests.post(EXTENSIONS_URL, json=json_data, headers=HEADERS)
    for x in response.json()["results"][0]["extensions"][0]["versions"]:
        print(x["version"])
