package example.modelcrew;
import java.time.LocalDateTime;


public class FlightVO {

    private int flightNumber;
    private String suffix;
    private LocalDateTime scheduleDate;
    private String departureStation;
    private String arrivalStation;


    public LocalDateTime getScheduleDate() {
        return scheduleDate;
    }


    public void setScheduleDate(LocalDateTime scheduleDate) {
        this.scheduleDate = scheduleDate;
    }

    public int getFlightNumber() {
        return flightNumber;
    }

    public void setFlightNumber(int flightNumber) {
        this.flightNumber = flightNumber;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public String getDepartureStation() {
        return departureStation;
    }


    public void setDepartureStation(String departureStation) {
        this.departureStation = departureStation;
    }

    public String getArrivalStation() {
        return arrivalStation;
    }


    public void setArrivalStation(String arrivalStation) {
        this.arrivalStation = arrivalStation;
    }

    @Override
    public String toString() {
        return "FlightVO{" +
                "flightNumber=" + flightNumber +
                ", suffix='" + suffix + '\'' +
                ", scheduleDate=" + scheduleDate +
                ", departureStation='" + departureStation + '\'' +
                ", arrivalStation='" + arrivalStation + '\'' +
                '}';
    }
}