import storageManager.Field;
import storageManager.FieldType;
import storageManager.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lsr on 2016/12/3.
 */
public class Heap {
    List<String> compare_field;
    int max_size;
    int size;
    Tuple_with_position[] heap;

    public Heap(int max_size, List<String> compare_field) {
        this.max_size = max_size;
        this.size = 0;
        this.compare_field = compare_field;
        heap = new Tuple_with_position[max_size];
    }

    public void insert(Tuple_with_position tuple){
            size++;
            heap[size-1] = tuple;
            int current = size-1;
            while(parent(current)>=0 && compare_tuple(heap[current].tuple,heap[parent(current)].tuple)<0){
                    swap(current,parent(current));
                    current = parent(current);
            }

    }

    public Tuple_with_position pop_min(){
        if(heap.length == 0){
            System.out.println("heap is empty");
            return null;
        }
        if(size == 0 ){
            System.out.print("empty");
            return null;
        }

        swap(0,size-1);
        size--;
        if(size == 0){
            return heap[0];
        }
        int current = 0;
        int small;
        while(!isLeaf(current)){
            small = rightchild(current);
            if(small<size) {
                if (compare_tuple(heap[small].tuple, heap[small - 1].tuple) > 0) {
                    small = small - 1;
                    if (compare_tuple(heap[current].tuple, heap[small].tuple) > 0) {
                        swap(current, small);
                        current = small;
                    } else {
                        break;
                    }
                } else{
                    if (compare_tuple(heap[current].tuple, heap[small].tuple) > 0) {
                        swap(current, small);
                        current = small;
                    } else {
                        break;
                    }

                }
            }else{
                small = small -1;
                if (compare_tuple(heap[current].tuple, heap[small].tuple) > 0) {
                    swap(current, small);
                    current = small;
                } else {
                    break;
                }
            }
        }
        return heap[size];
    }

    private boolean isLeaf(int pos){
        if(pos>=size/2 && pos<size){
            return true;
        }else{
            return false;
        }
    }

    private void swap(int index1,int index2){
        Tuple_with_position temp = heap[index1];
        heap[index1] = heap[index2];
        heap[index2] = temp;
    }

    private int leftchild(int pos){
        return (pos+1)*2-1;
    }

    private int rightchild(int pos){
        return (pos+1)*2;
    }

    private int parent(int pos){
        if(pos==0){
            return -1;
        }else{
            return (pos-1)/2;
        }
    }

    public int compare_tuple(Tuple t1, Tuple t2){
        if(compare_field.size()==1){
//            order by
            Field field1 = t1.getField(compare_field.get(0));
            Field field2 = t2.getField(compare_field.get(0));
            if(field1.type == FieldType.INT){
                return field1.integer-field2.integer;
            }else{
                return field1.str.compareTo(field2.str);
            }
        }else{

            for(int i=0; i<compare_field.size();i++){
                Field field1 = t1.getField(compare_field.get(i));
                Field field2 = t2.getField(compare_field.get(i));
                if(field1.type == FieldType.INT){
                    int res = field1.integer-field2.integer;
                    if(res!=0){
                        return res;
                    }
                }else{
                    int res =  field1.str.compareTo(field2.str);
                    if(res!=0){
                        return res;
                    }
                }
            }
            return 0;
        }
    }


}