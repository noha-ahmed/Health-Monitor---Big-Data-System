import * as React from "react";
import { styled } from "@mui/material/styles";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell, { tableCellClasses } from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Paper from "@mui/material/Paper";
import Button from "@mui/material/Button";
import axios from "axios"

const StyledTableCell = styled(TableCell)(({ theme }) => ({
  [`&.${tableCellClasses.head}`]: {
    backgroundColor: theme.palette.common.black,
    color: theme.palette.common.white,
  },
  [`&.${tableCellClasses.body}`]: {
    fontSize: 14,
  },
}));

const StyledTableRow = styled(TableRow)(({ theme }) => ({
  "&:nth-of-type(odd)": {
    backgroundColor: theme.palette.action.hover,
  },
  // hide last border
  "&:last-child td, &:last-child th": {
    border: 0,
  },
}));





export default function Statistics({ history ,location}) {
  const [values, setValues] = React.useState(location.state.data);
  const handleClick = () => {
    
    history.push("/");
  };



  return (
    <div>
      <Button
        variant=""
        style={{ border: "solid", font: "white" }}
        onClick={handleClick}
      >
       return Home
      </Button>

      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 700 }} aria-label="customized table">
          <TableHead>
            <TableRow>
          
              <StyledTableCell>Services</StyledTableCell>
              <StyledTableCell align="right">meanCpuUtilization</StyledTableCell>
              <StyledTableCell align="right">peakTimeCpu</StyledTableCell>
              <StyledTableCell align="right">meanRamUtilization</StyledTableCell>
              <StyledTableCell align="right">peakTimeRam</StyledTableCell>
              <StyledTableCell align="right">meanDiskUtilization</StyledTableCell>
              <StyledTableCell align="right">peakTimeDisk</StyledTableCell>
              <StyledTableCell align="right">messagesCount</StyledTableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {values.map((value) => (
              <StyledTableRow key={value.serviceName}>
                <StyledTableCell component="th" scope="value">
                  {value.serviceName}
                </StyledTableCell>
                <StyledTableCell align="right">{value.meanCpuUtilization}</StyledTableCell>
                <StyledTableCell align="right">{value.peakTimeCpu}</StyledTableCell>
                <StyledTableCell align="right">{value.meanRamUtilization}</StyledTableCell>
                <StyledTableCell align="right">{value.peakTimeRam}</StyledTableCell>
                <StyledTableCell align="right">{value.meanDiskUtilization}</StyledTableCell>
                <StyledTableCell align="right">{value.peakTimeDisk}</StyledTableCell>
                <StyledTableCell align="right">{value.messagesCount}</StyledTableCell>
              </StyledTableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
}
