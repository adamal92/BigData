Chart.defaults.global.defaultFontFamily = "Roboto, sans-serif";

async function mineJson(get_json){
  return get_json().then( 
    async function(response){
      var json_promise = response.json();

      // get mined json from within the json promise
      var mined_json = null;
      async function mineFunction(){
        mined_json = await json_promise.then(data => mined_json = data);
      }
      await mineFunction();

      console.log(mined_json);
      return mined_json;        
    }
  )    
}

// Data generation
async function getData() {
  const url = 'http://127.0.0.1:5000/get_json_visualization';
  console.log("YEYYYYYY");
  const response = await fetch(url);
  const json_ret = await response.json();
  //state.data = json_ret;
  console.log(json_ret);
  return json_ret;
  /*
  const get_json = async () => {
    const promise = await fetch(url)
    .catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"));

    return promise;
  }

  let data = [];

  json_ret = await mineJson(get_json).then(
  // get value from promise
  
    (json_ret) => {
      data.push({
        title: 'Visits',
        data: [{label: "k", value: 5}]
      });

      data.push({
        title: 'json',
        data: [json_ret]
      });
    }
  );
  */
  console.log(data);
  return data;
}

// BarChart
class BarChart extends React.Component {
  constructor(props) {
    super(props);
    this.canvasRef = React.createRef();
  }

  componentDidUpdate() {
    // this.myChart.data.labels = this.props.data.map(d => d.label);
    // this.myChart.data.datasets[0].data = this.props.data.map(d => d.value);
    this.myChart.update();

    // this.myChart.data.labels = this.props.data;
    // this.myChart.data.datasets[0].data = this.props.data;
    // this.myChart.update();

    // this.myChart.data.labels = "this.props.data";
    // this.myChart.data.datasets[0].data = this.props.data;
    // this.myChart.update();
  }

  componentDidMount() {
    // console.log("data: "+this.props.data);
    // console.log(this.props.title);
    // console.log(this.props.data);
    // console.log(this.props.color);
    var retrieved_data = [];
    
    for (const [key, value] of Object.entries(this.props.data)) {
      console.log(key, value);  // print the data to the console
      retrieved_data.push(value);
    }

    for(const val of retrieved_data){
      console.log(val);
    }

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
      // data: {
      //   labels: this.props.data.map(d => d.label),
      //   datasets: [{
      //     label: this.props.title,
      //     data: this.props.data.map(d => d.value),
      //     backgroundColor: this.props.color
      //   }]
      // }

      data: {
        labels: retrieved_data,  //-----------------------------------------!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Error
        datasets: [{
          label: this.props.title,
          data: retrieved_data,
          backgroundColor: this.props.color
        }]
      }

    // data: {
    //   // labels: "this.props.data",
    //   datasets: [{
    //     label: "this.props.title",
    //     // data: {"a":2},
    //     backgroundColor: "#B08EA2"
    //   }]
    // }
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
  
  state = { data: {}, isLoading: true };
  Url= 'http://127.0.0.1:5000/get_json_visualization';


  getJson = () => {
    fetch(this.Url, {})
      .then(data => data.json())
      .then(data => {
        console.log(data);
        this.setState({ data: data, isLoading: false });
      })
      .catch(e => console.log(e));
    };

  constructor(props) {
    super(props);

    // this.state = {}
    // async function func(state){
    //   state.data = await getData();
    // }

    // func(this.state);
    // console.log(this.state);
  }

  // updates the data constantly, while true
  componentDidMount() {
    // window.setInterval(() => {
    //   this.setState({
    //     data: getData()
    //   })
    // }, 5000);

    // this.getJson();
    window.setInterval(() => {
      this.getJson();
    }, 5000);
  }
  
  componentWillUnmount() {
    // fix Warning: Can't perform a React state update on an unmounted component
    this.setState = (state,callback)=>{
        return;
    };
  }

  render() {
    const { data, isLoading } = this.state;

    if (isLoading) {
      return null;
    }
    console.log(data, this.state.data);

    return (
      <div className="App">
        <div className="sub chart-wrapper">
          <BarChart
            data={this.state.data}
            title={"got json"}
            color="#B08EA2"
          />
        </div>
        {/* <div className="sub chart-wrapper">
          <BarChart
            data={this.state.data[1].data}
            title={this.state.data[1].title}
            color="#B00EA2"
          />
        </div> */}
      </div>
    );
  }
}

ReactDOM.render(<App />, document.getElementById('root'));
