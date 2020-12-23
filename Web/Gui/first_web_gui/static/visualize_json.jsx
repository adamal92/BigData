Chart.defaults.global.defaultFontFamily = "Roboto, sans-serif";

// Data generation
function getData() {
  const url = 'http://127.0.0.1:5000/get_json_visualization';

  const get_json = async () => {
    const response = await fetch(url)
    .then(got => console.log(got))
    .catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"));

    return response;
  }

  let data = [];

  data.push({
    title: 'Visits',
    data: [{label: "k", value: 5}]
  });

  // console.log(get_json().then(response => response));

  data.push({
    title: 'json',
    data: [get_json().then(response => response.json())]
  });

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
  componentDidMount() {
    window.setInterval(() => {
      this.setState({
        data: getData()
      })
    }, 5000)
  }

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
