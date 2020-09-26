# extract version notes for version VER
  
/^[*]+[-_0-9a-zA-Z]+ +- +/ {
    if ($4 == VER) {
        good = 1
        next
    } else {
        good = 0
    }
}

/^(===|---)/ { next }

{
    if (good) {
        print $0;
    }
}

