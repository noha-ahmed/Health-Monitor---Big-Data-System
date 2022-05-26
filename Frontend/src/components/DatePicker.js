import * as React from "react";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import AdapterDateFns from "@mui/lab/AdapterDateFns";
import LocalizationProvider from "@mui/lab/LocalizationProvider";
import DesktopDatePicker from "@mui/lab/DesktopDatePicker";
import Button from "@mui/material/Button";
import axios from "axios";

export default function DatePicker({ history }) {
  const [value1, setValue1] = React.useState(new Date("2014-08-18T21:11:54"));
  const [value2, setValue2] = React.useState(new Date("2014-08-18T21:11:54"));
  const [values, setValues] = React.useState([]);

  const handleChange1 = (newValue) => {
    setValue1(newValue);
    console.log( (new Date(newValue)).getTime());
  };
  const handleChange2 = (newValue) => {
    setValue2(newValue);
  };
  // const getData = async () => {
  //   const services = await axios.get(
  //     `http://localhost:8080/api/getStatistics/${ (new Date(value1)).getTime()}/${(new Date(value2)).getTime()}`
  //   );
  //   setValues(services);
  // };
  function makeGetRequest(path) {
    return new Promise(function (resolve, reject) {
        axios.get(path).then(
            (response) => {
                var result = response.data;
                console.log('Processing Request');
                resolve(result);
            },
                (error) => {
                reject(error);
            }
        );
    });
}
  
async function getData() {
    var result = await makeGetRequest( `http://localhost:8080/api/getStatistics/${ (new Date(value1)).getTime()}/${(new Date(value2)).getTime()}`);
    console.log(result);
    console.log('Statement 2');
    setValues(result);
    history.push("/Statistics" ,{ data: result });
}
  const handleClick = () => {
    
    getData();
    // history.push("/Statistics" ,{ fromDate: (new Date(value1)).getTime(), toDate:  (new Date(value2)).getTime() });
    // history.push("/Statistics" ,{ data: values });
  };
  return (
    <LocalizationProvider dateAdapter={AdapterDateFns}>
      <Stack spacing={4} style={{ marginTop: "100px" }}>
        <DesktopDatePicker
          label="from date"
          inputFormat="MM/dd/yyyy"
          value={value1}
          onChange={handleChange1}
          renderInput={(params) => <TextField {...params} />}
        />
        <DesktopDatePicker
          label="to date"
          inputFormat="MM/dd/yyyy"
          value={value2}
          onChange={handleChange2}
          renderInput={(params) => <TextField {...params} />}
        />
        <Button
          variant=""
          style={{ border: "solid", font: "white" }}
          onClick={handleClick}
        >
          Show statistics
        </Button>
      </Stack>
    </LocalizationProvider>
  );
}
