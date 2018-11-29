/*
Jiatai Han & Qifan Li
11/2018

This class defines heap
with name and its data type;
Should be just INT and STR20.
*/

import java.util.List;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.Tuple;

public class Heap {
    int size;
    int max_size;
    List<String> fields_to_be_compared;
    TuplePointer[] heap;

    public Heap(int max_size, List<String> fields_to_be_compared){
        heap = new TuplePointer[max_size];
        this.size = 0;
        this.max_size = max_size;
        this.fields_to_be_compared = fields_to_be_compared;
    }

    public void insert(TuplePointer tuple){
        size = size + 1;
        heap[size - 1] = tuple;
        int index_current = size - 1;
        while(parent(index_current) >= 0 && compare_tuple(heap[index_current].tuple,heap[parent(index_current)].tuple) < 0){
            exchange(index_current, parent(index_current));
            index_current = parent(index_current);
        }

    }

    public TuplePointer pop_min(){
        if (heap.length == 0 || size == 0){
            System.out.println("Heap is empty");
            return null;
        }
        exchange(0,size - 1);
        size = size - 1;
        if (size == 0){
            return heap[0];
        }
        int index_current = 0;
        int index_smaller = 0;
        while(!isLeaf(index_current)){
            index_smaller = child_right(index_current);
            if (index_smaller < size) {
                if (compare_tuple(heap[index_smaller].tuple, heap[index_smaller - 1].tuple) > 0) {
                    index_smaller = index_smaller - 1;
                    if (compare_tuple(heap[index_current].tuple, heap[index_smaller].tuple) > 0) {
                        exchange(index_current, index_smaller);
                        index_current = index_smaller;
                    }
                    else {break;}
                }
                else {
                    if (compare_tuple(heap[index_current].tuple, heap[index_smaller].tuple) > 0) {
                        exchange(index_current, index_smaller);
                        index_current = index_smaller;
                    }
                    else {break;}
                }
            }
            else {
                index_smaller = index_smaller -1;
                if (compare_tuple(heap[index_current].tuple, heap[index_smaller].tuple) > 0) {
                    exchange(index_current, index_smaller);
                    index_current = index_smaller;
                }
                else {break;}
            }
        }
        return heap[size];
    }

    private boolean isLeaf(int index){
        if (index >= size/2 && index < size){return true;}
        else {return false;}
    }

    private void exchange(int index1, int index2){
        TuplePointer temp = heap[index1];
        heap[index1] = heap[index2];
        heap[index2] = temp;
    }

    private int child_left(int index){return (index + 1) * 2 - 1;}
    private int child_right(int index){return (index + 1) * 2; }

    private int parent(int index){
        if (index == 0){return -1;}
        else {return (index - 1)/2;}
    }

    public int compare_tuple(Tuple t1, Tuple t2){
        if (fields_to_be_compared.size() == 1){
            Field field1 = t1.getField(fields_to_be_compared.get(0));
            Field field2 = t2.getField(fields_to_be_compared.get(0));
            if (field1.type == FieldType.INT){return field1.integer - field2.integer;}
            else {return field1.str.compareTo(field2.str);}
        }
        else {
            for(int i = 0; i < fields_to_be_compared.size(); i++){
                Field field1 = t1.getField(fields_to_be_compared.get(i));
                Field field2 = t2.getField(fields_to_be_compared.get(i));
                if (field1.type == FieldType.INT){
                    int res = field1.integer - field2.integer;
                    if (res != 0){return res;}
                }
                else {
                    int res =  field1.str.compareTo(field2.str);
                    if (res != 0){return res;}
                }
            }
            return 0;
        }
    }
}