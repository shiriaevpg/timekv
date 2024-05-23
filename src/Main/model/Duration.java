package Main.model;
class Duration{
    long time;
    Duration(long milliseconds){
        time = milliseconds;
    }
    static Duration Milliseconds(int milliseconds){
        return new Duration(milliseconds);
    }
    static Duration Seconds(int seconds){
        return new Duration(seconds * 1l * 1000);
    }
    static Duration Minutes(int minutes){
        return new Duration(minutes * 1l * 60 * 1000);
    }
    static Duration Hours(int hours){
        return new Duration(hours * 1l * 3600 * 1000);
    }
    static Duration Days(int days){
        return new Duration(days * 1l * 24 * 3600 * 1000);
    }
    static Duration Weeks(int weeks){
        return new Duration(weeks * 1l * 7 * 24 * 3600 * 1000);
    }
    static Duration Months(int months){
        return new Duration(months * 1l * 30 * 24 * 3600 * 1000);
    }
    long Get(){
        return time;
    }
}