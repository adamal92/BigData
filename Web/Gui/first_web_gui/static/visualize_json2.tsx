// Chart.defaults.global.defaultFontFamily = "Roboto, sans-serif";

// // Data generation
// function getData() {
//   const url = 'http://127.0.0.1:5000/get_json_visualization';

//   const get_json = async () => {
//     const promise = await fetch(url)
//     // .then(got_response => console.log(got_response))
//     .catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"));

//     return promise;
//   }

//   let data = [];
//   let push_to = null;

//   data.push({
//     title: 'Visits',
//     data: [{label: "k", value: 5}]
//   });

//   // console.log(get_json());
//   // console.log(get_json().then(response => response));
//   // console.log(get_json().then(response => console.log(response)));

//   data.push({
//     title: 'json',
//     data: [get_json().then( function(response: Response){
//       // console.log(response);
//       var json_promise: Promise<any> = response.json();
//       // console.log(json_promise);
//       // console.log(response.json());
//       // var json = json_promise.then(res => console.log(res));
//       json_promise.then(res =>{ 
//         push_to = res;
//         return res;
//       });
//       return response;
//     })]
//   });

//   return data;
// }

// // BarChart
// class BarChart extends React.Component {
//     canvasRef: React.RefObject<unknown>;
//     myChart: any;

//   constructor(props) {
//     super(props);
//     this.canvasRef = React.createRef();
//   }

//   componentDidUpdate() {
//     this.myChart.data.labels = this.props.data.map(d => d.label);
//     this.myChart.data.datasets[0].data = this.props.data.map(d => d.value);
//     this.myChart.update();
//   }

//   componentDidMount() {
//     this.myChart = new Chart(this.canvasRef.current, {
//       type: 'bar',
//       options: {
// 	      maintainAspectRatio: false,
//         scales: {
//           yAxes: [
//             {
//               ticks: {
//                 min: 0,
//                 max: 100
//               }
//             }
//           ]
//         }
//       },
//       data: {
//         labels: this.props.data.map(d => d.label),
//         datasets: [{
//           label: this.props.title,
//           data: this.props.data.map(d => d.value),
//           backgroundColor: this.props.color
//         }]
//       }
//     });
//   }

//   render() {
//     return (
//         <canvas ref={this.canvasRef} />
//     );
//   }
// }

// // App
// class App extends React.Component {
//   constructor(props) {
//     super(props);

//     this.state = {
//       data: getData()
//     };
//   }

//   // updates the data constantly, while true
//   /*
//   componentDidMount() {
//     window.setInterval(() => {
//       this.setState({
//         data: getData()
//       })
//     }, 5000)
//   }
//   */

//   render() {
//     return (
//       <div className="App">
//         <div className="sub chart-wrapper">
//           <BarChart
//             data={this.state.data[0].data}
//             title={this.state.data[0].title}
//             color="#B08EA2"
//           />
//         </div>
//       </div>
//     );
//   }
// }

// ReactDOM.render(<App />, document.getElementById('root'));
