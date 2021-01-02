const url = 'http://127.0.0.1:5000/get_json';

const func = async () => {
  const response = await fetch(url)
  .then(response => response.text())
  .then(contents => console.log(contents))
  .catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"));

  // const data = await response.text;

  // console.log(data);
}

// func();
console.log("ok crawler");


/*
try{
  // Get file from elastic
  fetch("http://localhost:9200/school/_doc/quotes")
    .then(response => response.json())
    .then(data => console.log(data))

}catch(e){
  console.log(e);
}*/
/*
const url = "http://localhost:9200/school/_doc/quotes"; // site that doesn’t send Access-Control-*
const proxyurl = "https://cors-anywhere.herokuapp.com/";
// res.header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by");

let headers = new Headers();
headers.append('Content-Type', 'application/json');
headers.append('Accept', 'application/json');

headers.append('Access-Control-Allow-Origin', 'http://localhost:5000');
headers.append('Access-Control-Allow-Credentials', 'true');
headers.append('Access-Control-Allow-Headers', 'x-requested-with, x-requested-by');

headers.append('GET', 'POST', 'OPTIONS');

// fetch(url)
fetch(url, {mode: 'no-cors', method: 'GET', headers: headers}) // https://cors-anywhere.herokuapp.com/https://example.com
.then(response => response.text())
.then(contents => console.log(contents))
.catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"))
// TODO: get json from server
*/
// fetch("http://localhost:5000/get_json")
// .then(response => response.text())
// .then(contents => console.log(contents))
// .catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"))



/*
try{
    const Http = new XMLHttpRequest();
    const url='https://jsonplaceholder.typicode.com/posts';
    Http.open("GET", url);
    Http.send();

    Http.onreadystatechange = (e) => {
      console.log(Http.responseText)
    }

}catch(e){
    console.log(e);
}

const Http = new XMLHttpRequest();
const url='https://jsonplaceholder.typicode.com/posts';
Http.open("GET", url);
Http.send();

Http.onreadystatechange = (e) => {
  console.log(Http.responseText)
}



const Url='';

$('.button').click(function(){

    console.log("ok crawler");

    $.get(Url, function(data, status){
        console.log(`${data}`)
    });
})
*/