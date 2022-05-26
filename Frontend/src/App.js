import './App.css';
import { useHistory ,  useLocation} from "react-router-dom";
import { BrowserRouter as Router, Route, Switch } from "react-router-dom";
import Home from './components/Home';
import Statistics from './components/Statistics';
function App() {
  let history = useHistory();
  let location = useLocation();

  return (
    <div className="App">
      <Switch>
        <Route exact path="/">
          <Home history ={history } location={ location}/>
        </Route>
        <Route path="/Statistics">
          <Statistics history ={history} location={ location} />
        </Route>
        
      </Switch>

    </div>
  );
}

export default App;
