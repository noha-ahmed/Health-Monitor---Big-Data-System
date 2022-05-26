import * as React from "react";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import DatePicker from "./DatePicker";
import "../App.css";

export default function Home({ history }) {
  return (
    <div>
      <div className="App-header">
        <h style={{ marginTop: "100px" }}>Health Monitoring System</h>
        <DatePicker history={history} />
      </div>
    </div>
  );
}
