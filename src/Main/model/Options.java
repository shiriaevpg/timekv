package Main.model;

import java.util.Optional;

public class Options{
    public Duration interval;
    public Optional<Integer> maxBytesSize;
    public Optional<Duration> maxAge;
    public boolean store_raw = false;
}