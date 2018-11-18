import storageManager.Tuple;

public class Tuple_with_position {
      public Tuple tuple;
      public int sublist_pointer;
      public int block_pointer;
      public int tuple_pointer;
      public Tuple_with_position(Tuple tuple, int sublist_pointer, int block_pointer, int tuple_pointer){
    	  this.tuple=tuple;
    	  this.sublist_pointer=sublist_pointer;
    	  this.block_pointer=block_pointer;
    	  this.tuple_pointer=tuple_pointer;
      }
      
}
