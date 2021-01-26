'use strict';

const e = React.createElement;

function visualise(data){
  console.log(data);
  console.log(typeof data);
  return String(data);
}

class GetJSONButton extends React.Component {
  constructor(props) {
    super(props);
    this.state = { pressed: false };
  }

  render() {
    const url = 'http://127.0.0.1:5000/get_json';
    var k;
    const get_json = async () => {
      const response = await fetch(url)
      // .then(response => response.text())
      // .then(function(contents){ 
      //   k = visualise(contents);        
      //  })
      .catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"));

      return response;
    }

    if (this.state.pressed) {
      var g = get_json();
      return String(g
        .then(response => console.log(response.text())));
    }

    return e(
      'button',
      { onClick: () => this.setState({ pressed: true }) },
      'get json'
    );
  }
}

// -------------------------------------------
const domContainer = document.querySelector('#get_json_button_container');
ReactDOM.render(e(GetJSONButton), domContainer);