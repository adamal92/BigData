const url = 'http://127.0.0.1:5000/moto_spider';

fetch(url)
  .then(response => response.text())
  .then(contents => console.log(contents))
  .catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"));