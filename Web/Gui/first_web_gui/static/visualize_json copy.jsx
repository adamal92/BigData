Chart.defaults.global.defaultFontFamily = "Roboto, sans-serif";

function mineJson(get_json){
   // console.log(get_json());
  // console.log(get_json().then(response => response));
  // console.log(get_json().then(response => console.log(response)));

  const func = async () => {
    var my_json_promise = get_json().then( 
      async function(response){
        var json_promise = response.json();
        var my_json = null;
        async function myFunction(){
          my_json = await json_promise.then(data => my_json = data);
        }
        await myFunction();
        console.log(my_json);
        return my_json;        
        // console.log(response);
        // console.log(json_promise);
        // console.log(response.json());
        // var json = json_promise.then(res => console.log(res));
        // console.log(json_promise);
        // json_promise.then(res => console.log(res));
        // var json_pr = json_promise.then(res =>{ 
        //   // push_to = res;
        //   return res;
        // });
        // await json_promise.then(data => my_json = data);
        // const prin = async() =>{ var json_file = await json_pr; return json_file;}
        // console.log(prin());
       
      }
    )
    var json_final = null;
    async function get_json_final(){
      json_final = await my_json_promise.then(data => json_final = data);
    }
    await get_json_final();
    console.log(json_final);
    return json_final;      
  }

  return func();
}

// Data generation
function getData() {
  const url = 'http://127.0.0.1:5000/get_json_visualization';

  const get_json = async () => {
    const promise = await fetch(url)
    // .then(got_response => console.log(got_response))
    .catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"));

    return promise;
  }

  let data = [];
  let push_to = null;

  data.push({
    title: 'Visits',
    data: [{label: "k", value: 5}]
  });

  // get value from promise
  async function funcc(){
    // var promise = mineJson(get_json);
    // var json = null;
    // async function get_json(){
    //   json = await promise.then(data => json = data);
    // }
    // await get_json();
    // console.log(json);

    data.push({
      title: 'json',
    
      data: [json]
    });
  }

  funcc();
  // json = mineJson(get_json);

  // console.log(json);

  // data.push({
  //   title: 'json',
   
  //   data: [json]
  // });

  return data;
}

// BarChart
class BarChart extends React.Component {
  constructor(props) {
    super(props);
    this.canvasRef = React.createRef();
  }

  componentDidUpdate() {
    this.myChart.data.labels = this.props.data.map(d => d.label);
    this.myChart.data.datasets[0].data = this.props.data.map(d => d.value);
    this.myChart.update();
  }

  componentDidMount() {
    this.myChart = new Chart(this.canvasRef.current, {
      type: 'bar',
      options: {
	      maintainAspectRatio: false,
        scales: {
          yAxes: [
            {
              ticks: {
                min: 0,
                max: 100
              }
            }
          ]
        }
      },
      data: {
        labels: this.props.data.map(d => d.label),
        datasets: [{
          label: this.props.title,
          data: this.props.data.map(d => d.value),
          backgroundColor: this.props.color
        }]
      }
    });
  }

  render() {
    return (
        <canvas ref={this.canvasRef} />
    );
  }
}

// App
class App extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      data: getData()
    };
  }

  // updates the data constantly, while true
  /*
  componentDidMount() {
    window.setInterval(() => {
      this.setState({
        data: getData()
      })
    }, 5000)
  }
  */

  render() {
    return (
      <div className="App">
        <div className="sub chart-wrapper">
          <BarChart
            data={this.state.data[0].data}
            title={this.state.data[0].title}
            color="#B08EA2"
          />
        </div>
      </div>
    );
  }
}

ReactDOM.render(<App />, document.getElementById('root'));
