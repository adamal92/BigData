import urllib.request

# URL_GOV = 'https://data.gov.il/api/3/action/datastore_search?resource_id=dcf999c1-d394-4b57-a5e0-9d014a62e046'
URL_GOV = 'https://data.gov.il/api/3/action/datastore_search?' \
          'resource_id=8a21d39d-91e3-40db-aca1-f73f7ab1df69&limit=3564' \
          '&q=filter:C'
fileobj = urllib.request.urlopen(URL_GOV)
print(fileobj.read())

# with urllib.request.urlopen(URL_GOV) as url:
#     s = url.read()
#     # I'm guessing this would output the html source code ?
#     print(s)
