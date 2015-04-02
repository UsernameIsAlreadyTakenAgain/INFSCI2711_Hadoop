package Recommendation;


import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

//rank TreeMap by value
class ValueComparator implements Comparator<Long> {

  Map<Long, List> base;
  
  public ValueComparator(Map<Long, List> base) {
      this.base = base;
  }
  
  // Note: this comparator imposes orderings that are inconsistent with equals.    
  public int compare(Long a, Long b) {
      if (base.get(a).size() >base.get(b).size()) {
          return -1;
      } 
      if(base.get(a).size() < base.get(b).size()) {
          return 1;
      } 
      
      return a.compareTo(b);// returning 0 would merge keys

}
}
