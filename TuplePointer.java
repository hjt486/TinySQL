import storageManager.Tuple;

public class TuplePointer {
      Tuple tuple;
      int sub_pointer;
      int block_pointer;
      int tuple_pointer;

      public TuplePointer(Tuple tuple, int sub_pointer, int block_pointer, int tuple_pointer){
    	  this.tuple = tuple;
    	  this.sub_pointer = sub_pointer;
    	  this.block_pointer = block_pointer;
    	  this.tuple_pointer = tuple_pointer;
      }
      
}
