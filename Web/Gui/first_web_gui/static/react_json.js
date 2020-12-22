'use strict';

const e = React.createElement;

class GetJSONButton extends React.Component {
  constructor(props) {
    super(props);
    this.state = { pressed: false };
  }

  render() {
    const url = 'http://127.0.0.1:5000/get_json';

    const get_json = async () => {
      var content;

      const response = await fetch(url)
      .then(response => response.text())
      .then(contents => console.log(contents))
      .catch(() => console.log("Can’t access " + url + " response. Blocked by browser?"));

      return response;
    }

    if (this.state.pressed) {
      return get_json().text();
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