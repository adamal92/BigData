// Custom js
console.log("script");

// try{
//   // Get user with id 2
//   fetch('https://jsonplaceholder.typicode.com/users/2')
//     .then(response => response.json())
//     .then(data => console.log(data))

//   document.addEventListener('DOMContentLoaded', function() {
//       var url = 'http://127.0.0.1:5001/GUI-is-still-open';
//       fetch(url, { mode: 'no-cors'});
//       setInterval(function(){ fetch(url, { mode: 'no-cors'});}, 5000)();
//   });

// }catch(e){
//   console.log(e);
// }


/*try{
    const Http = new XMLHttpRequest();
    const url='https://jsonplaceholder.typicode.com/posts';
    Http.open("GET", url);
    Http.send();

    Http.onreadystatechange = (e) => {
      console.log(Http.responseText)
    }

}catch(e){
    console.log(e);
}*/
/*
// Basic blueprint
fecth(url)
  .then(response.something) // Define response type (JSON, Headers, Status codes)
  .then(data) // get the response type

// Practical example
fetch('https://jsonplaceholder.typicode.com/todos')
  .then(response => response.json())
  .then(data => console.log(JSON.stringify(data)))
  */
