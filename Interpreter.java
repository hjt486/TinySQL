/*
Jiatai Han & Qifan Li
11/2018

This class defines interpreter object,
Any query will use thos class to execute.
*/

import java.util.ArrayList;
import java.util.regex.Pattern;
import storageManager.*;

public class Interpreter {
	public MainMemory mem;
	public Disk disk;
	public SchemaManager schema_manager;
	private Parser parse;

	public Interpreter(){
		mem = new MainMemory();
		disk = new Disk();
		schema_manager = new SchemaManager(mem, disk);
		disk.resetDiskIOs();
		disk.resetDiskTimer();
		parse = new Parser();
	}

	public void execute(String query){
		if (parse.syntax(query) == true){
			switch (parse.key_word.get(0).toUpperCase()) {
				case "CREATE": create(); break;
				case "DROP": drop(); break;
				case "INSERT":
					insert();
					if (schema_manager.relationExists("relation_returned")) {schema_manager.deleteRelation("relation_returned");}
					if (schema_manager.relationExists("relation_new")) {schema_manager.deleteRelation("relation_new");}
					break;
				case "DELETE": delete(); break;
				case "SELECT":
					Relation relation = select();
					System.out.println(relation);
					if (relation.getRelationName().contains(",")) {schema_manager.deleteRelation(relation.getRelationName());}
					if (schema_manager.relationExists("relation_new")) {schema_manager.deleteRelation("relation_new");}
					if (schema_manager.relationExists("relation_returned")) {schema_manager.deleteRelation("relation_returned");}
					if (parse.select.distinct) {
						if (schema_manager.relationExists("relation_order")) {schema_manager.deleteRelation("relation_distinct");}
					}
					if (parse.select.order) {
						if (schema_manager.relationExists("relation_order")) {schema_manager.deleteRelation("relation_order"); }
					}
					if (schema_manager.relationExists("op_temp")) {schema_manager.deleteRelation("op_temp");}
					break;
				default: System.out.println("Input Syntax Error! Please Check"); break;
			}
		}
		else {System.out.println("Input Syntax Error, Please Check!");}
	}

	public void create(){
		ArrayList<String> attribute_names = new ArrayList<>();
		ArrayList<FieldType> attribute_types = new ArrayList<>();
		for (int i = 0; i < parse.arg.size(); i++){
			attribute_names.add(parse.arg.get(i).name);
			switch (parse.arg.get(i).type.toUpperCase()){
				case "STR20": attribute_types.add(FieldType.STR20); break;
				case "INT": attribute_types.add(FieldType.INT); break;
				default: System.out.println("Wrong input type!");
			}
		}
		Schema schema = new Schema(attribute_names, attribute_types);
		schema_manager.createRelation(parse.table_name.get(0), schema);
		System.out.println("Table \"" + parse.table_name.get(0) + "\" is created.");
	}

	public void drop(){
		String table = parse.table_name.get(0);
		schema_manager.deleteRelation(table);
		System.out.println("Table \"" + table + "\" is deleted.");
	}

	public void insert(){
		String table_name = parse.table_name.get(0);
		if (schema_manager.relationExists(table_name) == false) {System.out.println("Table " + table_name + " doesn't exist!");}
		Relation relation = schema_manager.getRelation(table_name);
		Tuple tuple = relation.createTuple();
		Schema relation_schema = relation.getSchema();
		if (parse.select == null){
			boolean field_type_verify = false;
			for (int i = 0; i < tuple.getNumOfFields(); i++) {
				try {
					if (relation_schema.getFieldType(parse.arg.get(i).name) == FieldType.STR20) {
						String value = "";
						if (parse.values.get(i).equalsIgnoreCase("NULL")) {tuple.setField(parse.arg.get(i).name, value);}
						else {
							value = parse.values.get(i).replaceAll("\"", "");
							tuple.setField(parse.arg.get(i).name, value);
						}
					}
					else {
						if (parse.values.get(i).equalsIgnoreCase("NULL")) {tuple.setField(parse.arg.get(i).name, -21321321);}
						else {tuple.setField(parse.arg.get(i).name, Integer.parseInt(parse.values.get(i)));}
					}
					field_type_verify = true;
				}
				catch (NumberFormatException e) {
					System.out.println("Wrong format! Field " + parse.arg.get(i).name + " is of INT type!");
					System.out.println("You're trying to insert a string value.");
					field_type_verify = false;
					break;
				}
			}
			if (field_type_verify == true) {
				appendTupleToRelation(relation,mem,2,tuple);
				System.out.println("A row is inserted.");
			}
		}
		else if (parse.select != null){
			Relation selected_relation = select();
			Schema schema = new Schema(selected_relation.getSchema());
			Relation relation_new = schema_manager.createRelation("new_temp", schema);
			for (int i = 0; i < selected_relation.getNumOfBlocks(); i++){
				selected_relation.getBlock(i, 9);
				relation_new.setBlock(i, 9);
			}
			int formerBlocks = relation.getNumOfBlocks();
			for (int i=0; i < relation_new.getNumOfBlocks(); i++){
				relation_new.getBlock(i,9);
				relation.setBlock(i + formerBlocks,9);
			}
		}
	}

	public void delete(){
		String table_name = parse.delete.tables.get(0);
		Relation relation = schema_manager.getRelation(table_name);
		int table_blocks_count = relation.getNumOfBlocks();
		if (table_blocks_count == 0) {System.out.println("Table \"" + table_name + "\" is empty!"); }
		else {
			int scan_count;
			if ((table_blocks_count % Config.NUM_OF_BLOCKS_IN_MEMORY) != 0) {scan_count = table_blocks_count / Config.NUM_OF_BLOCKS_IN_MEMORY + 1;}
			else {scan_count = table_blocks_count / Config.NUM_OF_BLOCKS_IN_MEMORY;}
			for (int i = 0; i < scan_count; i++){
				int num_blocks;
				if (table_blocks_count - i * Config.NUM_OF_BLOCKS_IN_MEMORY <= Config.NUM_OF_BLOCKS_IN_MEMORY) {num_blocks = table_blocks_count - i * Config.NUM_OF_BLOCKS_IN_MEMORY;}
				else {num_blocks = Config.NUM_OF_BLOCKS_IN_MEMORY;}
				relation.getBlocks(i * Config.NUM_OF_BLOCKS_IN_MEMORY, 0, num_blocks);
				NodeGenerator deleteTree = parse.delete.where_clause;
				ArrayList<Block> blocks = new ArrayList<>();
				for (int j = 0; j < num_blocks; j++){
					Block block = mem.getBlock(j);
					if (deleteTree != null) {
						ArrayList<Tuple> tuples_tempe = block.getTuples();
						if (tuples_tempe.size() != 0) {for (int k = 0; k < tuples_tempe.size(); k++) {if (deleteTree == null || where_decide(deleteTree, tuples_tempe.get(k))){block.invalidateTuple(k);}}}
						if (block.getNumTuples() == 0) {block.clear();}
					}
					else {block.clear();}
					if (!block.isEmpty()) {blocks.add(block); }
				}
				for (int j = 0; j < blocks.size(); j++) {mem.setBlock(j, blocks.get(j));}
				relation.setBlocks(i * Config.NUM_OF_BLOCKS_IN_MEMORY, 0, num_blocks);
				relation.deleteBlocks(blocks.size());
			}
		}
	}

	public Relation select(){
		Relation table_joined;
		boolean doOne_pass = true;
		for (int i = 0; i < parse.select.tables.size(); i++){
			if (schema_manager.getRelation(parse.select.tables.get(i)).getNumOfBlocks() != 1) {doOne_pass = false;}
		}
		if (doOne_pass && parse.select.tables.size() > 1){
			table_joined = one_pass(parse.select.tables);
			if (parse.select.where_clause == null && parse.select.arguments.get(0).equalsIgnoreCase("*")){
				if (parse.select.distinct){
					table_joined = two_pass_1st_round(table_joined, table_joined.getSchema().getFieldNames());
					table_joined = two_pass_2nd_round(table_joined, table_joined.getSchema().getFieldNames());
				}
				if (parse.select.order == true){
					ArrayList<String> attribute_order = new ArrayList<>();
					String order_by = parse.select.order_clause.trim();
					attribute_order.add(order_by);
					table_joined = two_pass_1st_round(table_joined, attribute_order);
					if (table_joined.getNumOfBlocks() > 10) {table_joined = order_second_pass(table_joined, attribute_order);}
				}
				return table_joined;
			}
		}
		else {
			if (parse.select.tables.size() == 1){
				Schema schema_new = schema_manager.getSchema(parse.select.tables.get(0));
				table_joined = schema_manager.createRelation("relation_new", schema_new);
				int blocks = schema_manager.getRelation(parse.select.tables.get(0)).getNumOfBlocks();
				int scan_count;
				if (blocks % 9 == 0) {scan_count = blocks / 9;}
				else {scan_count = blocks / 9 + 1;}
				for (int i = 0; i < scan_count; i++){
					if ((i + 1) * 9 > blocks){
						schema_manager.getRelation(parse.select.tables.get(0)).getBlocks(i * 9, 0, blocks - ( 9 * i));
						table_joined.setBlocks(i * 9, 0, blocks-(9 * i));
					}
					else {
						schema_manager.getRelation(parse.select.tables.get(0)).getBlocks(i * 9, 0, 9);
						table_joined.setBlocks(i * 9, 0, 9);
					}
				}
				if (parse.select.where_clause == null && parse.select.arguments.get(0).equalsIgnoreCase("*")){
					if (parse.select.distinct){
						table_joined = two_pass_1st_round(table_joined, table_joined.getSchema().getFieldNames());
						table_joined = two_pass_2nd_round(table_joined, table_joined.getSchema().getFieldNames());
					}
					if (parse.select.order){
						ArrayList<String> attributes_order = new ArrayList<String>();
						String order_by = parse.select.order_clause.trim();
						attributes_order.add(order_by);
						table_joined = two_pass_1st_round(table_joined, attributes_order);
						if (table_joined.getNumOfBlocks() > 10) {table_joined = order_second_pass(table_joined, attributes_order);}
					}
					return table_joined;}
				}
			else {
				String table_previous = parse.select.tables.get(0);
				String table_now;
				boolean isLast_one = false;
				for (int i = 1; i < parse.select.tables.size(); i++){
					if (i == (parse.select.tables.size()-1)) {isLast_one = true;}
					table_now = parse.select.tables.get(i);
					String temp = table_previous;
					boolean isNatural_join = false;
					if (i == 1) {isNatural_join = true;}
					table_previous = join_new(table_previous,table_now,isLast_one,isNatural_join);
					if (temp.contains(",")) {schema_manager.deleteRelation(temp);}
				}
				table_joined = schema_manager.getRelation(table_previous);
			}
			if (parse.select.where_clause == null && parse.select.arguments.get(0).equalsIgnoreCase("*")){
				if (parse.select.distinct == true){
					table_joined = two_pass_1st_round(table_joined, table_joined.getSchema().getFieldNames());
					table_joined = two_pass_2nd_round(table_joined, table_joined.getSchema().getFieldNames());
				}
				if (parse.select.order == true){
					ArrayList<String> attributes_order = new ArrayList<>();
					String order_by = parse.select.order_clause.trim();
					attributes_order.add(order_by);
					table_joined = two_pass_1st_round(table_joined, attributes_order);
					if (table_joined.getNumOfBlocks() > 10) {table_joined = order_second_pass(table_joined, attributes_order);}
				}
				return table_joined;
			}
		}
		Schema schema_returned;
		Relation relation_returned;
		if (parse.select.tables.size()>1){
			ArrayList<String> attribute_names_returned = new ArrayList<String>();
			ArrayList<FieldType> attribute_types_returned = new ArrayList<FieldType>();
			if (parse.select.arguments.get(0).equalsIgnoreCase("*")){
				attribute_names_returned = table_joined.getSchema().getFieldNames();
				attribute_types_returned = table_joined.getSchema().getFieldTypes();
			}
			else {
				attribute_names_returned = parse.select.arguments;
				for (int i = 0; i < attribute_names_returned.size(); i++) {attribute_types_returned.add(table_joined.getSchema().getFieldType(attribute_names_returned.get(i)));}
			}
			schema_returned = new Schema(attribute_names_returned,attribute_types_returned);
			relation_returned = schema_manager.createRelation("relation_returned", schema_returned);
		}
		else {
			ArrayList<String> attribute_names_returned = new ArrayList<>();
			ArrayList<FieldType> attribute_types_returned = new ArrayList<>();
			if (parse.select.arguments.get(0).equalsIgnoreCase("*")){
				attribute_names_returned = table_joined.getSchema().getFieldNames();
				attribute_types_returned = table_joined.getSchema().getFieldTypes();
			}
			else {
				attribute_names_returned = parse.select.arguments;
				for (int i = 0; i < attribute_names_returned.size(); i++){
					if (attribute_names_returned.get(i).split("\\.").length == 1) {attribute_types_returned.add(table_joined.getSchema().getFieldType(attribute_names_returned.get(i))); }
					else {
						String real_name = attribute_names_returned.get(i).split("\\.")[1];
						attribute_types_returned.add(table_joined.getSchema().getFieldType(real_name));
					}
				}
			}
			schema_returned = new Schema(attribute_names_returned,attribute_types_returned);
			relation_returned = schema_manager.createRelation("relation_returned", schema_returned);
		}
		int t_blocks = table_joined.getNumOfBlocks();
		if (t_blocks == 0) {System.out.println("The table is empty!");}
		int scan_count;
		if ((t_blocks%(Config.NUM_OF_BLOCKS_IN_MEMORY-1))!=0) {scan_count = t_blocks/(Config.NUM_OF_BLOCKS_IN_MEMORY-1)+1;}
		else {scan_count = t_blocks/(Config.NUM_OF_BLOCKS_IN_MEMORY-1);}
		for (int i = 0; i < scan_count; i++){
			int num_blocks;
			if (t_blocks-i*(Config.NUM_OF_BLOCKS_IN_MEMORY-1)<(Config.NUM_OF_BLOCKS_IN_MEMORY-1)) {num_blocks = t_blocks-(i*(Config.NUM_OF_BLOCKS_IN_MEMORY-1));}
			else {num_blocks = Config.NUM_OF_BLOCKS_IN_MEMORY-1;}
			table_joined.getBlocks(i*(Config.NUM_OF_BLOCKS_IN_MEMORY-1), 0, num_blocks);
			NodeGenerator Tree_Selected = parse.select.where_clause;
			for (int j = 0; j < num_blocks; j++){
				Block block = mem.getBlock(j);
				ArrayList<Tuple> tuples_tempe = block.getTuples();
				if (tuples_tempe.size() == 0) continue;
				for (int k=0; k < tuples_tempe.size(); k++){
					if (Tree_Selected == null || where_decide(Tree_Selected,tuples_tempe.get(k))){
						if (parse.select.arguments.get(0).equalsIgnoreCase("*")) {appendTupleToRelation(relation_returned,mem,9,tuples_tempe.get(k)); }
						else {
							Tuple tuple_returned = relation_returned.createTuple();
							for (int n = 0; n < parse.select.arguments.size(); n++){
								if (parse.select.tables.size() == 1){
									String[] table_attributes = parse.select.arguments.get(n).split("\\.");
									if (table_attributes.length == 1){
										if (schema_returned.getFieldType(parse.select.arguments.get(n)) == FieldType.STR20) {tuple_returned.setField(parse.select.arguments.get(n),block.getTuple(k).getField(parse.select.arguments.get(n)).str);}
										else {tuple_returned.setField(parse.select.arguments.get(n),block.getTuple(k).getField(parse.select.arguments.get(n)).integer);}
									}
									else {
										if (schema_returned.getFieldType(parse.select.arguments.get(n))==FieldType.STR20) {tuple_returned.setField(parse.select.arguments.get(n),block.getTuple(k).getField(table_attributes[1]).str);}
										else {tuple_returned.setField(parse.select.arguments.get(n),block.getTuple(k).getField(table_attributes[1]).integer);}
									}
								}
								else {
									if (schema_returned.getFieldType(parse.select.arguments.get(n)) == FieldType.STR20) {tuple_returned.setField(parse.select.arguments.get(n),block.getTuple(k).getField(parse.select.arguments.get(n)).str);}
									else {tuple_returned.setField(parse.select.arguments.get(n),block.getTuple(k).getField(parse.select.arguments.get(n)).integer);}
								}
							}
							appendTupleToRelation(relation_returned,mem,9,tuple_returned);
						}
					}
				}
			}
		}
		if (parse.select.distinct){
			relation_returned = two_pass_1st_round(relation_returned, relation_returned.getSchema().getFieldNames());
			relation_returned = two_pass_2nd_round(relation_returned, relation_returned.getSchema().getFieldNames());
		}
		if (parse.select.order){
			ArrayList<String> attributes_order = new ArrayList<>();
			String order_by = parse.select.order_clause.trim();
			attributes_order.add(order_by);
			relation_returned = two_pass_1st_round(relation_returned, attributes_order);
			if (relation_returned.getNumOfBlocks() > 10) {relation_returned=order_second_pass(relation_returned, attributes_order);}
		}
		if (parse.select.tables.size()>1) {schema_manager.deleteRelation(table_joined.getRelationName());}
		return relation_returned;
	}

	private Relation one_pass(ArrayList<String> tables){
		Schema schema_one_pass = schema_merged(tables);
		Relation op_temp = schema_manager.createRelation("op_temp", schema_one_pass);
		for (int j = 0; j < tables.size(); j++) {schema_manager.getRelation(tables.get(j)).getBlock(0, j);}
		ArrayList<Tuple> tuples = new ArrayList<>();
		int nums = 1;
		for (int i = 0; i < tables.size(); i++) {nums = nums * (schema_manager.getRelation(tables.get(i)).getNumOfTuples());}
		for (int i = 0; i < nums; i++){
			Tuple table_temp = op_temp.createTuple();
			tuples.add(table_temp);
		}
		if (!parse.select.distinct && !parse.select.order && parse.select.where_clause == null && parse.select.arguments.get(0).equalsIgnoreCase("*")){
			System.out.println(schema_one_pass);
		}
		tuples = one_pass_memory(tuples,tables.size(), 0, tables, nums);
		for (int i = 0; i < tuples.size(); i++){
			if (!parse.select.distinct && !parse.select.order && parse.select.where_clause == null && parse.select.arguments.get(0).equalsIgnoreCase("*")){
				System.out.println(tuples.get(i));
			}
			else {appendTupleToRelation(op_temp, mem,9, tuples.get(i));}
		}
		return op_temp;
	}

	private ArrayList<Tuple> one_pass_memory(ArrayList<Tuple> tuples,int counts, int current_count, ArrayList<String> tables, int total_table_counts){
		if (current_count == (counts - 1)){
			int tuple_counts = schema_manager.getRelation(tables.get(current_count)).getNumOfTuples();
			int each_tuple = total_table_counts / tuple_counts;
			int total_attribute_counts = tuples.get(0).getNumOfFields();
			for (int i = 0; i < tuple_counts; i++){
				for (int j = i * each_tuple; j < (each_tuple * (i + 1)); j++){
					int attribute_counts = schema_manager.getSchema(tables.get(current_count)).getNumOfFields();
					for (int k = 0; k < attribute_counts; k++){
						if (tuples.get(j).getField(total_attribute_counts - 1 - k).type == FieldType.STR20){
							tuples.get(j).setField(total_attribute_counts - 1 - k,mem.getBlock(current_count).getTuple(i).getField(attribute_counts - k - 1).str);}
						else {tuples.get(j).setField(total_attribute_counts - 1 - k,mem.getBlock(current_count).getTuple(i).getField(attribute_counts - k - 1).integer);}
					}
				}
			}
			return tuples;
		}
		tuples = one_pass_memory(tuples,counts,current_count+1,tables,total_table_counts);
		int tuple_counts = schema_manager.getRelation(tables.get(current_count)).getNumOfTuples();
		int each_tuple = total_table_counts / tuple_counts;
		int total_attribute_counts = tuples.get(0).getNumOfFields();
		int previous_attribute_counts = 0;
		for (int i = 0; i < current_count; i++) {previous_attribute_counts = schema_manager.getSchema(tables.get(i)).getNumOfFields() + previous_attribute_counts;}
		for (int i = 0; i < tuple_counts; i++){
			for (int j = i * each_tuple; j < (each_tuple*(i+1)); j++){
				int attribute_counts = schema_manager.getSchema(tables.get(current_count)).getNumOfFields();
				for (int k=0; k < attribute_counts; k++){
					if (tuples.get(j).getField(previous_attribute_counts + k).type == FieldType.STR20){
						tuples.get(j).setField(previous_attribute_counts + k, mem.getBlock(current_count).getTuple(i).getField(k).str);}
					else {
						tuples.get(j).setField(previous_attribute_counts + k,mem.getBlock(current_count).getTuple(i).getField(k).integer);}
				}
			}
		}
		return tuples;
	}
	
	private Schema schema_merged(ArrayList<String> tables){
		Schema[] schemas = new Schema[tables.size()];
		ArrayList<String> attribute_joined_names = new ArrayList<>();
		ArrayList<FieldType> attribute_joined_types = new ArrayList<>();
		for (int i = 0; i < schemas.length; i++){
			schemas[i] = schema_manager.getSchema(tables.get(i));
			ArrayList<String> attribute_names = schemas[i].getFieldNames();
			for (int j = 0; j < attribute_names.size(); j++){
				String name_new = tables.get(i) + "." + attribute_names.get(j);
				attribute_names.set(j, name_new);
			}
			attribute_joined_names.addAll(attribute_names);
			attribute_joined_types.addAll(schemas[i].getFieldTypes());
		}
		Schema schema_joined = new Schema(attribute_joined_names, attribute_joined_types);
		return schema_joined;
	}
	
	private String natural_join(String table1, String table2, String attribute){
		Relation relation1 = schema_manager.getRelation(table1);
		Relation relation2 = schema_manager.getRelation(table2);
		ArrayList<FieldType> attribute_types = relation1.getSchema().getFieldTypes();
		ArrayList<FieldType> attribute_types2 = relation2.getSchema().getFieldTypes();
		attribute_types.addAll(attribute_types2);
		ArrayList<String> attribute_names_new = new ArrayList<String>();
		if (relation1.getSchema().getFieldNames().get(0).contains(".")) {attribute_names_new = relation1.getSchema().getFieldNames();}
		else {
			for (int i=0; i < relation1.getSchema().getNumOfFields(); i++){
				String attribute_name = table1 + "." + relation1.getSchema().getFieldNames().get(i);
				attribute_names_new.add(attribute_name);
			}
		}
		if (relation2.getSchema().getFieldNames().get(0).contains(".")) {attribute_names_new.addAll(relation2.getSchema().getFieldNames());}
		else {
			for (int i = 0; i < relation2.getSchema().getNumOfFields(); i++){
				String attribute_name = table2 + "." + relation2.getSchema().getFieldNames().get(i);
				attribute_names_new.add(attribute_name);
			}
		}
		String natural_join_name = table1 + "," + table2;
		Schema natural_join_schema = new Schema(attribute_names_new,attribute_types);
		Relation natural_join_relation = schema_manager.createRelation(natural_join_name, natural_join_schema);
		ArrayList<String> natural_join_attribute = new ArrayList<>();
		natural_join_attribute.add(attribute);
		relation1 = two_pass_1st_round(relation1,natural_join_attribute);
		relation2 = two_pass_1st_round(relation2,natural_join_attribute);
		Heap heap1 = new Heap(80,natural_join_attribute);
		Heap heap2 = new Heap(80,natural_join_attribute);
		int relation1_blocks = relation1.getNumOfBlocks();
		int relation2_blocks = relation2.getNumOfBlocks();
		int round_counts1 = 0;
		int round_counts2 = 0;
		if (relation1_blocks%Config.NUM_OF_BLOCKS_IN_MEMORY == 0) {round_counts1 = relation1_blocks / Config.NUM_OF_BLOCKS_IN_MEMORY;}
		else {round_counts1 = relation1_blocks/Config.NUM_OF_BLOCKS_IN_MEMORY + 1;}
		if (relation2_blocks%Config.NUM_OF_BLOCKS_IN_MEMORY == 0) {round_counts2 = relation2_blocks / Config.NUM_OF_BLOCKS_IN_MEMORY;}
		else {round_counts2 = relation2_blocks / Config.NUM_OF_BLOCKS_IN_MEMORY + 1;}
		for (int i = 0; i < round_counts1; i++){
			relation1.getBlock(i*Config.NUM_OF_BLOCKS_IN_MEMORY, i);
			TuplePointer tuple_p = new TuplePointer(mem.getBlock(i).getTuple(0),i,0,0);
			heap1.insert(tuple_p);
		}
		for (int i = 0; i < round_counts2; i++){
			relation2.getBlock(i*Config.NUM_OF_BLOCKS_IN_MEMORY, i + round_counts1);
			TuplePointer tuple_p = new TuplePointer(mem.getBlock(i + round_counts1).getTuple(0),i + round_counts1,0,0);
			heap2.insert(tuple_p);
		}
		while(heap1.size > 0 && heap2.size > 0){
			TuplePointer tuple_p1 = heap1.pop_min();
			TuplePointer tuple_p2 = heap2.pop_min();
			heap1.insert(tuple_p1);
			heap2.insert(tuple_p2);
			if (tuple_p1.tuple.getField(attribute).integer > tuple_p2.tuple.getField(attribute).integer){
				TuplePointer tuple_pointer_temp = heap2.pop_min();
				if (tuple_pointer_temp.tuple_pointer < mem.getBlock(tuple_pointer_temp.sub_pointer).getNumTuples() - 1){
					Tuple tuple = mem.getBlock(tuple_pointer_temp.sub_pointer).getTuple(tuple_pointer_temp.tuple_pointer + 1);
					heap2.insert(new TuplePointer(tuple,tuple_pointer_temp.sub_pointer, tuple_pointer_temp.block_pointer, tuple_pointer_temp.tuple_pointer + 1));
				}
				else if (tuple_pointer_temp.block_pointer < 9 && (tuple_pointer_temp.sub_pointer - round_counts1)*10 + tuple_pointer_temp.block_pointer<relation2_blocks - 1){
					tuple_pointer_temp.block_pointer++;
					relation2.getBlock((tuple_pointer_temp.sub_pointer - round_counts1) * 10 + tuple_pointer_temp.block_pointer,tuple_pointer_temp.sub_pointer);
					heap2.insert(new TuplePointer(mem.getBlock(tuple_pointer_temp.sub_pointer).getTuple(0), tuple_pointer_temp.sub_pointer, tuple_pointer_temp.block_pointer,0));
				}
			}
			else if (tuple_p1.tuple.getField(attribute).integer < tuple_p2.tuple.getField(attribute).integer){
				TuplePointer tuple_pointer_temp = heap1.pop_min();
				if (tuple_pointer_temp.tuple_pointer < mem.getBlock(tuple_pointer_temp.sub_pointer).getNumTuples() - 1){
					Tuple tuple = mem.getBlock(tuple_pointer_temp.sub_pointer).getTuple(tuple_pointer_temp.tuple_pointer + 1);
					heap1.insert(new TuplePointer(tuple,tuple_pointer_temp.sub_pointer, tuple_pointer_temp.block_pointer, tuple_pointer_temp.tuple_pointer + 1));
				}
				else if (tuple_pointer_temp.block_pointer < 9 && tuple_pointer_temp.sub_pointer * 10 + tuple_pointer_temp.block_pointer < relation1_blocks - 1){
					tuple_pointer_temp.block_pointer++;
					relation1.getBlock(tuple_pointer_temp.sub_pointer * 10 + tuple_pointer_temp.block_pointer, tuple_pointer_temp.sub_pointer);
					heap1.insert(new TuplePointer(mem.getBlock(tuple_pointer_temp.sub_pointer).getTuple(0), tuple_pointer_temp.sub_pointer, tuple_pointer_temp.block_pointer, 0));
				}
			}
			else {
				TuplePointer tuple_p1_temp = heap1.pop_min();
				TuplePointer tuple_p2_temp = heap2.pop_min();
				if (tuple_p2_temp.tuple_pointer < mem.getBlock(tuple_p2_temp.sub_pointer).getNumTuples() - 1){
					Tuple tuple = mem.getBlock(tuple_p2_temp.sub_pointer).getTuple(tuple_p2_temp.tuple_pointer + 1);
					heap2.insert(new TuplePointer(tuple,tuple_p2_temp.sub_pointer, tuple_p2_temp.block_pointer, tuple_p2_temp.tuple_pointer + 1));
				}
				else if (tuple_p2_temp.block_pointer < 9 && (tuple_p2_temp.sub_pointer - round_counts1) * 10 + tuple_p2_temp.block_pointer < relation2_blocks - 1){
					tuple_p2_temp.block_pointer++;
					relation2.getBlock((tuple_p2_temp.sub_pointer -round_counts1) * 10 + tuple_p2_temp.block_pointer, tuple_p2_temp.sub_pointer);
					heap2.insert(new TuplePointer(mem.getBlock(tuple_p2_temp.sub_pointer).getTuple(0), tuple_p2_temp.sub_pointer, tuple_p2_temp.block_pointer, 0));
				}
				if (tuple_p1_temp.tuple_pointer < mem.getBlock(tuple_p1_temp.sub_pointer).getNumTuples() - 1){
					Tuple tuple = mem.getBlock(tuple_p1_temp.sub_pointer).getTuple(tuple_p1_temp.tuple_pointer + 1);
					heap1.insert(new TuplePointer(tuple, tuple_p1_temp.sub_pointer, tuple_p1_temp.block_pointer, tuple_p1_temp.tuple_pointer + 1));
				}
				else if (tuple_p1_temp.block_pointer < 9 && tuple_p1_temp.sub_pointer * 10 + tuple_p1_temp.block_pointer < relation1_blocks - 1){
					tuple_p1_temp.block_pointer++;
					relation1.getBlock(tuple_p1_temp.sub_pointer * 10 + tuple_p1_temp.block_pointer, tuple_p1_temp.sub_pointer);
					heap1.insert(new TuplePointer(mem.getBlock(tuple_p1_temp.sub_pointer).getTuple(0), tuple_p1_temp.sub_pointer, tuple_p1_temp.block_pointer, 0));
				}
				Tuple tuple1 = tuple_p1_temp.tuple;
				Tuple tuple2 = tuple_p2_temp.tuple;
				Tuple natural_join_tuple = natural_join_relation.createTuple();
				int table1_attributes = tuple1.getNumOfFields();
				for (int k = 0; k < table1_attributes; k++){
					if (tuple1.getField(k).type == FieldType.INT) {natural_join_tuple.setField(k,tuple1.getField(k).integer);}
					else {natural_join_tuple.setField(k,tuple1.getField(k).str);}
				}
				for (int n = 0; n < tuple2.getNumOfFields(); n++){
					if (tuple2.getField(n).type == FieldType.INT) {natural_join_tuple.setField(n+table1_attributes,tuple2.getField(n).integer);}
					else {natural_join_tuple.setField(n+table1_attributes,tuple2.getField(n).str);}
				}
				appendTupleToRelation(natural_join_relation, mem, 9, natural_join_tuple);
				TuplePointer pop1 = heap1.pop_min();
				TuplePointer pop2 = heap2.pop_min();
				Tuple compare1 = pop1.tuple;
				Tuple compare2 = pop2.tuple;
				heap1.insert(pop1);
				heap2.insert(pop2);
				while(heap1.size > 0 && heap1.compare_tuple(tuple1, compare1) == 0){
					TuplePointer new_tuple_pointer1 = heap1.pop_min();
					Tuple tuple_new1 = new_tuple_pointer1.tuple;
					table1_attributes = tuple_new1.getNumOfFields();
					Tuple new_natural_join_tuple = natural_join_relation.createTuple();
					for (int k = 0; k < table1_attributes; k++){
						if (tuple_new1.getField(k).type == FieldType.INT) {new_natural_join_tuple.setField(k, tuple_new1.getField(k).integer);}
						else {new_natural_join_tuple.setField(k, tuple_new1.getField(k).str);}
					}
					for (int n = 0; n < tuple2.getNumOfFields(); n++){
						if (tuple2.getField(n).type == FieldType.INT) {new_natural_join_tuple.setField(n+table1_attributes,tuple2.getField(n).integer);}
						else {new_natural_join_tuple.setField(n+table1_attributes,tuple2.getField(n).str);}
					}
					appendTupleToRelation(natural_join_relation, mem,9, new_natural_join_tuple);

					TuplePointer tuple_pointer_temp = new_tuple_pointer1;
					if (tuple_pointer_temp.tuple_pointer < mem.getBlock(tuple_pointer_temp.sub_pointer).getNumTuples() - 1){
						Tuple tuple = mem.getBlock(tuple_pointer_temp.sub_pointer).getTuple(tuple_pointer_temp.tuple_pointer + 1);
						heap1.insert(new TuplePointer(tuple, tuple_pointer_temp.sub_pointer, tuple_pointer_temp.block_pointer, tuple_pointer_temp.tuple_pointer + 1));
					}
					else if (tuple_pointer_temp.block_pointer < 9 && tuple_pointer_temp.sub_pointer * 10 + tuple_pointer_temp.block_pointer < relation1_blocks - 1){
						tuple_pointer_temp.block_pointer++;
						relation1.getBlock(tuple_pointer_temp.sub_pointer * 10 + tuple_pointer_temp.block_pointer, tuple_pointer_temp.sub_pointer);
						heap1.insert(new TuplePointer(mem.getBlock(tuple_pointer_temp.sub_pointer).getTuple(0), tuple_pointer_temp.sub_pointer, tuple_pointer_temp.block_pointer, 0));
					}
					if (heap1.size > 0){
						TuplePointer temp_tp1 = heap1.pop_min();
						heap1.insert(temp_tp1);
						compare1 = temp_tp1.tuple;
					}
				}
				while(heap2.size > 0 && heap2.compare_tuple(tuple2, compare2) == 0){
					TuplePointer new_tuple_pointer2 = heap2.pop_min();
					Tuple tuple_new2 = new_tuple_pointer2.tuple;
					Tuple new_natural_join_tuple = natural_join_relation.createTuple();
					for (int k=0; k<table1_attributes; k++){
						if (tuple1.getField(k).type == FieldType.INT){new_natural_join_tuple.setField(k,tuple1.getField(k).integer);}
						else {new_natural_join_tuple.setField(k, tuple1.getField(k).str);}
					}
					for (int n=0;n<tuple_new2.getNumOfFields();n++){
						if (tuple_new2.getField(n).type == FieldType.INT) {new_natural_join_tuple.setField(n+table1_attributes, tuple_new2.getField(n).integer);}
						else {new_natural_join_tuple.setField(n+table1_attributes, tuple_new2.getField(n).str);}
					}
					appendTupleToRelation(natural_join_relation,mem, 9, new_natural_join_tuple);

					TuplePointer tuple_pointer_temp = new_tuple_pointer2;
					if (tuple_pointer_temp.tuple_pointer < mem.getBlock(tuple_pointer_temp.sub_pointer).getNumTuples() - 1){
						Tuple tuple = mem.getBlock(tuple_pointer_temp.sub_pointer).getTuple(tuple_pointer_temp.tuple_pointer + 1);
						heap2.insert(new TuplePointer(tuple, tuple_pointer_temp.sub_pointer, tuple_pointer_temp.block_pointer, tuple_pointer_temp.tuple_pointer + 1));
					}
					else if (tuple_pointer_temp.block_pointer < 9 && (tuple_pointer_temp.sub_pointer - round_counts1) * 10 + tuple_pointer_temp.block_pointer < relation2_blocks - 1){
						tuple_pointer_temp.block_pointer++;
						relation2.getBlock((tuple_pointer_temp.sub_pointer - round_counts1) * 10 + tuple_pointer_temp.block_pointer, tuple_pointer_temp.sub_pointer);
						heap2.insert(new TuplePointer(mem.getBlock(tuple_pointer_temp.sub_pointer).getTuple(0), tuple_pointer_temp.sub_pointer, tuple_pointer_temp.block_pointer,0));
					}
					if (heap2.size>0){
						TuplePointer temp_tp2 = heap2.pop_min();
						heap2.insert(temp_tp2);
						compare2 = temp_tp2.tuple;
					}
				}

			}
		}
		return natural_join_name;
	}

	private String join_new(String table1, String table2, boolean last, boolean natural_join){
		ArrayList<NodeGenerator> clause = new ArrayList<NodeGenerator>();
		if (parse.select.where_clause != null) clause = parse.select.where_clause.hasSelection();
		ArrayList<NodeGenerator> meet_clauses = new ArrayList<NodeGenerator>();
		ArrayList<String> table1_names = new ArrayList<String>();
		if (table1.contains(","))
		{
			String[] table1_names_s = table1.split(",");
			for (int i = 0; i < table1_names_s.length; i++){
				table1_names.add(table1_names_s[i]);
			}
		}
		for (int i = 0; i < clause.size(); i++){
			NodeGenerator tree = clause.get(i);
			boolean add_or_not = false;
			if (table1_names.size() > 0){
				for (int j = 0; j < table1_names.size(); j++){
					if ((tree.left.operator.contains(table1_names.get(j)) && tree.right.operator.contains(table2))) {add_or_not = true;}
				}
			}
			else {
				if (tree.left.operator.contains(table1) && tree.right.operator.contains(table2)){
					if (tree.left.operator.contains(table1) && tree.right.operator.contains(table2)){
						String op_left = tree.left.operator;
						String op_right = tree.right.operator;
						int index_left = op_left.indexOf(".");
						int index_right = op_right.indexOf(".");
						if (index_left > 0 && index_right > 0){
							String subtree_left = op_left.substring(index_left+1, op_left.length());
							String subtree_right = op_right.substring(index_right+1, op_right.length());
							if (subtree_left.equalsIgnoreCase(subtree_right) && natural_join) {return natural_join(table1,table2,subtree_left);}
						}
						add_or_not = true;
					}
				}
			}

			if (add_or_not == true){meet_clauses.add(tree);}
		}

		Relation relation1 = schema_manager.getRelation(table1);
		Relation relation2 = schema_manager.getRelation(table2);
		ArrayList<String> attribute_names_new = new ArrayList<String>();
		ArrayList<FieldType> fieldtypes_new = relation1.getSchema().getFieldTypes();
		fieldtypes_new.addAll(relation2.getSchema().getFieldTypes());
		if (relation1.getSchema().getFieldNames().get(0).contains(".")){attribute_names_new = relation1.getSchema().getFieldNames();}
		else {
			for (int i=0; i < relation1.getSchema().getNumOfFields(); i++){
				String temp_name = table1 + "." + relation1.getSchema().getFieldNames().get(i);
				attribute_names_new.add(temp_name);
			}
		}
		if (relation2.getSchema().getFieldNames().get(0).contains(".")){attribute_names_new.addAll(relation2.getSchema().getFieldNames());}
		else {
			for (int i=0; i < relation2.getSchema().getNumOfFields(); i++){
				String temp_name = table2 + "." + relation2.getSchema().getFieldNames().get(i);
				attribute_names_new.add(temp_name);
			}
		}
		Schema schema_new = new Schema(attribute_names_new,fieldtypes_new);
		if (last) System.out.println(schema_new);
		String table_new = table1 + "," + table2;
		Relation relation_new = schema_manager.createRelation(table_new, schema_new);
		int table1_blocks = relation1.getNumOfBlocks();
		int table1_memory;
		if (table1_blocks > Config.NUM_OF_BLOCKS_IN_MEMORY - 2) table1_memory = Config.NUM_OF_BLOCKS_IN_MEMORY-2;
		else table1_memory = table1_blocks;
		for (int i=0; i < table1_blocks; i += table1_memory){
			if (i + table1_memory > table1_blocks) table1_memory = table1_blocks - i;
			relation1.getBlocks(i, 0, table1_memory);
			for (int j=0;j<table1_memory;j++){
				Block table1_block = mem.getBlock(j);
				if (table1_block.isEmpty()) continue;
				int table2_blocks = relation2.getNumOfBlocks();
				for (int m = 0;m < table2_blocks; m++){
					relation2.getBlock(m, Config.NUM_OF_BLOCKS_IN_MEMORY-  2);
					Block table2_block = mem.getBlock(Config.NUM_OF_BLOCKS_IN_MEMORY - 2);
					if (table2_block.isEmpty()) continue;
					for (int k = 0; k < table1_block.getNumTuples();k++){
						Tuple table1_tuple = table1_block.getTuple(k);
						if (table1_tuple.isNull()) continue;
						for (int n = 0; n < table2_block.getNumTuples(); n++){
							Tuple table2_tuple = table2_block.getTuple(n);
							if (table2_tuple.isNull()) continue;
							Tuple tuple_joined = relation_new.createTuple();
							for (int t = 0; t < table1_tuple.getNumOfFields(); t++){
								if (table1_tuple.getField(t).type == FieldType.INT) {tuple_joined.setField(t, table1_tuple.getField(t).integer);}
								else if (table1_tuple.getField(t).type == FieldType.STR20) {tuple_joined.setField(t, table1_tuple.getField(t).str);}
							}
							for (int t = table1_tuple.getNumOfFields(); t<tuple_joined.getNumOfFields(); t++){
								if (table2_tuple.getField(t - table1_tuple.getNumOfFields()).type == FieldType.INT){
									tuple_joined.setField(t, table2_tuple.getField(t - table1_tuple.getNumOfFields()).integer);
								}
								else if (table2_tuple.getField(t - table1_tuple.getNumOfFields()).type == FieldType.STR20){
									tuple_joined.setField(t, table2_tuple.getField(t-table1_tuple.getNumOfFields()).str);
								}
							}
							if (meet_clauses.size() == 0) {
								appendTupleToRelation(relation_new, mem, Config.NUM_OF_BLOCKS_IN_MEMORY - 1, tuple_joined);
							}
							else {
								int counter = 0;
								for (int t = 0; t < meet_clauses.size(); t++){if (where_decide(meet_clauses.get(t), tuple_joined)){counter ++;}}
								if (counter == meet_clauses.size()){appendTupleToRelation(relation_new, mem, Config.NUM_OF_BLOCKS_IN_MEMORY-1, tuple_joined);}
							}
						}
					}
				}
			}
		}
		return table_new;
	}

	private boolean where_decide(NodeGenerator tree, Tuple tuple){
		if (tree == null) {return true;}
		if (calculator(tree, tuple).equalsIgnoreCase("true")) return true;
		else if (calculator(tree, tuple).equalsIgnoreCase("null")) System.out.println("Syntax error!!!");
		return false;
	}

	private String calculator(NodeGenerator tree, Tuple tuple){
		if (tree.left == null){return tree.operator;}
		if (tree.right ==null){return tree.operator;}
		String left = "false";
		String right = "false";
		if (tree.left != null) {left= calculator(tree.left, tuple);}
		if (tree.right != null) {right= calculator(tree.right,tuple);}
		if (tree.operator.equalsIgnoreCase("&")){
			if (left.equalsIgnoreCase("true") && right.equalsIgnoreCase("true")) {return "true";}
			else {return "false";}
		}
		else if (tree.operator.equalsIgnoreCase("|")){
			if (left.equalsIgnoreCase("true")||right.equalsIgnoreCase("true")) {return "true";}
			else {return "false";}
		}
		else if (tree.operator.equalsIgnoreCase("=")){
			if (Pattern.matches("[0-9]",String.valueOf(left.charAt(0)))){
				if (Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if (left.equalsIgnoreCase(right)) {return "true";}
					else return "false";}
				else if (Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					int value_left = Integer.parseInt(left);
					int value_right = tuple.getField(right).integer;
					if (value_left == value_right) return "true";
					else return "false";
				}
			}
			else if (Pattern.matches("[^0-9]",String.valueOf(left.charAt(0)))){
				if (Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					int value_right = Integer.parseInt(right);
					int value_left = tuple.getField(left).integer;
					if (value_left == value_right) return "true";
					else return "false";
				}
				else if (Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if (tuple.getSchema().fieldNameExists(right)) {
						if (tuple.getSchema().fieldNameExists(left)){
							if (tuple.getField(left).str != null){
								if (tuple.getField(right).str.equalsIgnoreCase(tuple.getField(left).str)) return "true";
								else return "false";
							}
							else {
								if (tuple.getField(right).integer == tuple.getField(left).integer) return "true";
								else return "false";
							}
						}
						else {
							left=left.replaceAll("\"", "");
							if (tuple.getField(right).str.equalsIgnoreCase(left)) return "true";
							else return "false";
						}
					}
					else if (!tuple.getSchema().fieldNameExists(right)){
						if (tuple.getSchema().fieldNameExists(left)){
							right=right.replaceAll("\"", "");
							if (tuple.getField(left).str.equalsIgnoreCase(right)) return "true";
							else return "false";
						}
						else {
							if (left.equalsIgnoreCase(right)) return "true";
							else return "false";
						}
					}
				}
			}
		}
		else if (tree.operator.equalsIgnoreCase("<")){
			if (Pattern.matches("[^0-9]", String.valueOf(left.charAt(0)))){
				if (Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if (tuple.getField(left).integer < tuple.getField(right).integer) return "true";
					else return "false";
				}
				else if (Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if (tuple.getField(left).integer < Integer.parseInt(right)) return "true";
					else return "false";
				}
			}
			else if (Pattern.matches("[0-9]", String.valueOf(left.charAt(0)))){
				if (Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if (Integer.parseInt(left) < tuple.getField(right).integer) return "true";
					else return "false";
				}
				else if (Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if (Integer.parseInt(left) < Integer.parseInt(right)) return "true";
					else return "false";
				}
			}
		}
		else if (tree.operator.equalsIgnoreCase(">")){
			if (Pattern.matches("[^0-9]", String.valueOf(left.charAt(0)))){
				if (Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if (tuple.getField(left).integer > tuple.getField(right).integer) return "true";
					else return "false";
				}
				else if (Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if (tuple.getField(left).integer > Integer.parseInt(right)) return "true";
					else return "false";
				}
			}
			else if (Pattern.matches("[0-9]", String.valueOf(left.charAt(0)))){
				if (Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if (Integer.parseInt(left) > tuple.getField(right).integer) return "true";
					else return "false";
				}
				else if (Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if (Integer.parseInt(left) > Integer.parseInt(right)) return "true";
					else return "false";
				}
			}
		}
		else if (tree.operator.equalsIgnoreCase("+")  || tree.operator.equalsIgnoreCase("-")  || tree.operator.equalsIgnoreCase("*")  || tree.operator.equalsIgnoreCase("/")){
			if (Pattern.matches("[0-9]",String.valueOf(left.charAt(0)))){
				if (Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					int result = 0;
					if (tree.operator.equalsIgnoreCase("+")){
						result = Integer.parseInt(left) + Integer.parseInt(right);}
					else if (tree.operator.equalsIgnoreCase("-")){
						result = Integer.parseInt(left) - Integer.parseInt(right);}
					else if (tree.operator.equalsIgnoreCase("*")){
						result = Integer.parseInt(left) * Integer.parseInt(right);}
					else if (tree.operator.equalsIgnoreCase("/")){
						result = Integer.parseInt(left) / Integer.parseInt(right);}
					return String.valueOf(result);
				}
				else if (Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					int value_right = tuple.getField(right).integer;
					int result = 0;
					if (tree.operator.equalsIgnoreCase("+")){
						result = Integer.parseInt(left) + value_right;}
					else if (tree.operator.equalsIgnoreCase("-")){
						result = Integer.parseInt(left) - value_right;}
					else if (tree.operator.equalsIgnoreCase("*")){
						result = Integer.parseInt(left) * value_right;}
					else if (tree.operator.equalsIgnoreCase("/")){
						result = Integer.parseInt(left) / value_right;}
					return String.valueOf(result);
				}
			}
			else if (Pattern.matches("[0-9]",String.valueOf(right.charAt(0)))){
				if (Pattern.matches("[^0-9]", String.valueOf(left.charAt(0)))){
					int value_left = tuple.getField(left).integer;
					int result = 0;
					if (tree.operator.equalsIgnoreCase("+")){
						result = Integer.parseInt(right) + value_left;}
					else if (tree.operator.equalsIgnoreCase("-")){
						result = Integer.parseInt(right) - value_left;}
					else if (tree.operator.equalsIgnoreCase("*")){
						result = Integer.parseInt(right) * value_left;}
					else if (tree.operator.equalsIgnoreCase("/")){
						result = Integer.parseInt(right) / value_left;}
					return String.valueOf(result);
				}
			}
			else if (Pattern.matches("[^0-9]",String.valueOf(left.charAt(0)))&&Pattern.matches("[^0-9]",String.valueOf(right.charAt(0)))){
				int value_left=tuple.getField(left).integer;
				int value_right=tuple.getField(right).integer;
				int result = 0;
				if (tree.operator.equalsIgnoreCase("+")){
					result = value_right + value_left;}
				else if (tree.operator.equalsIgnoreCase("-")){
					result = value_right - value_left;}
				else if (tree.operator.equalsIgnoreCase("*")){
					result = value_right * value_left;}
				else if (tree.operator.equalsIgnoreCase("/")){
					result = value_right / value_left;}
				return String.valueOf(result);
			}

		}
		return "null";
	}

	private Relation two_pass_1st_round(Relation relation_returned, ArrayList<String> attribute_names){
		Heap heap = new Heap(80,attribute_names);
		int blocks_count = relation_returned.getNumOfBlocks();
		int scan_count;
		if (blocks_count % Config.NUM_OF_BLOCKS_IN_MEMORY==0)  scan_count = blocks_count/Config.NUM_OF_BLOCKS_IN_MEMORY;
		else scan_count = blocks_count / Config.NUM_OF_BLOCKS_IN_MEMORY+  1;
		for (int i = 0; i < scan_count; i++){
			if (blocks_count - Config.NUM_OF_BLOCKS_IN_MEMORY * i >= Config.NUM_OF_BLOCKS_IN_MEMORY){
				relation_returned.getBlocks(i * Config.NUM_OF_BLOCKS_IN_MEMORY, 0, Config.NUM_OF_BLOCKS_IN_MEMORY);
				for (int j = 0; j < Config.NUM_OF_BLOCKS_IN_MEMORY; j++){
					Block block_sorted = mem.getBlock(j);
					if (block_sorted.isEmpty()) {continue;}
					for (int k=0;k<block_sorted.getNumTuples();k++){
						Tuple tuple_sorted = block_sorted.getTuple(k);
						if (tuple_sorted.isNull())  {continue;}
						TuplePointer tuple_pointer = new TuplePointer(tuple_sorted,0,0,0);
						heap.insert(tuple_pointer);
					}
				}
				for (int j = 0; j < Config.NUM_OF_BLOCKS_IN_MEMORY; j++){
					Block block_sorted = mem.getBlock((j));
					block_sorted.clear();
					while(!block_sorted.isFull() && heap.size>0){
						TuplePointer tuple_pointer = heap.pop_min();
						block_sorted.appendTuple(tuple_pointer.tuple);
					}
				}
				relation_returned.setBlocks(i * Config.NUM_OF_BLOCKS_IN_MEMORY, 0, Config.NUM_OF_BLOCKS_IN_MEMORY);
			}
			else {
				relation_returned.getBlocks(i*Config.NUM_OF_BLOCKS_IN_MEMORY, 0, blocks_count-i*Config.NUM_OF_BLOCKS_IN_MEMORY);
				for (int j=0;j<blocks_count-i*Config.NUM_OF_BLOCKS_IN_MEMORY;j++){
					Block block_sorted = mem.getBlock(j);
					if (block_sorted.isEmpty()) {continue;}
					for (int k = 0;k < block_sorted.getNumTuples();k++){
						Tuple tuple_sorted = block_sorted.getTuple(k);
						if (tuple_sorted.isNull())  {continue;}
						TuplePointer tuple_pointer = new TuplePointer(tuple_sorted,0,0,0);
						heap.insert(tuple_pointer);
					}
				}
				for (int j=0;j<blocks_count-i*Config.NUM_OF_BLOCKS_IN_MEMORY;j++){
					Block block_sorted=mem.getBlock((j));
					block_sorted.clear();
					while(!block_sorted.isFull() && heap.size>0){
						TuplePointer tuple_pointer= heap.pop_min();
						block_sorted.appendTuple(tuple_pointer.tuple);
					}
				}
				relation_returned.setBlocks(i * Config.NUM_OF_BLOCKS_IN_MEMORY, 0, blocks_count - i * Config.NUM_OF_BLOCKS_IN_MEMORY);
			}
		}
		return relation_returned;
	}

	private Relation two_pass_2nd_round(Relation relation_returned, ArrayList<String> attribute_names){
		Heap heap = new Heap(80,attribute_names);
		int blocks_count = relation_returned.getNumOfBlocks();
		int sub_count;
		if (blocks_count % Config.NUM_OF_BLOCKS_IN_MEMORY == 0) {sub_count=blocks_count/Config.NUM_OF_BLOCKS_IN_MEMORY;}
		else sub_count = blocks_count / Config.NUM_OF_BLOCKS_IN_MEMORY+1;
		for (int i = 0; i < sub_count; i++){
			relation_returned.getBlock(i * Config.NUM_OF_BLOCKS_IN_MEMORY, i);
		}
		for (int i = 0; i < sub_count; i++){
			Block block = mem.getBlock(i);
			Tuple tuple = block.getTuple(0);
			TuplePointer tuple_pointer = new TuplePointer(tuple, i, 0, 0);
			heap.insert(tuple_pointer);
		}
		if (schema_manager.relationExists("relation_distinct")){
			schema_manager.deleteRelation("relation_distinct");
		}
		Relation relation_distinct = schema_manager.createRelation("relation_distinct", relation_returned.getSchema());
		TuplePointer result = heap.pop_min();
		Tuple output_tuple = result.tuple;
		heap.insert(result);
		appendTupleToRelation(relation_distinct, mem, 9, output_tuple);
		while(heap.size>0)
		{
			TuplePointer tuple_pointer = heap.pop_min();
			if (heap.compare_tuple(tuple_pointer.tuple, result.tuple) != 0)
			{
				appendTupleToRelation(relation_distinct, mem, 9, tuple_pointer.tuple);
				result = tuple_pointer;
			}
			if (tuple_pointer.tuple_pointer<mem.getBlock(tuple_pointer.sub_pointer).getNumTuples()-1)
			{
				Tuple tuple = mem.getBlock(tuple_pointer.sub_pointer).getTuple(tuple_pointer.tuple_pointer+1);
				heap.insert(new TuplePointer(tuple,tuple_pointer.sub_pointer,tuple_pointer.block_pointer,tuple_pointer.tuple_pointer+1));
			}
			else if (tuple_pointer.block_pointer<9 && tuple_pointer.sub_pointer *10+tuple_pointer.block_pointer<blocks_count-1){//sublist not exhaust
				tuple_pointer.block_pointer++;
				relation_returned.getBlock(tuple_pointer.sub_pointer *10+tuple_pointer.block_pointer, tuple_pointer.sub_pointer);
				heap.insert(new TuplePointer(mem.getBlock(tuple_pointer.sub_pointer).getTuple(0),tuple_pointer.sub_pointer,tuple_pointer.block_pointer,0));
			}
		}
		return relation_distinct;
	}

	private Relation order_second_pass(Relation relation_returned, ArrayList<String> attributes_order){
		Heap heap = new Heap(80, attributes_order);
		int blocks_count = relation_returned.getNumOfBlocks();
		int sub_count;
		if (blocks_count%Config.NUM_OF_BLOCKS_IN_MEMORY==0) {sub_count=blocks_count/Config.NUM_OF_BLOCKS_IN_MEMORY;}
		else sub_count = blocks_count / Config.NUM_OF_BLOCKS_IN_MEMORY + 1;
		for (int i = 0; i < sub_count; i++){
			relation_returned.getBlock(i * Config.NUM_OF_BLOCKS_IN_MEMORY, i);
		}
		for (int i=0;i<sub_count;i++){
			Block block=mem.getBlock(i);
			Tuple tuple=block.getTuple(0);
			TuplePointer tuple_pointer = new TuplePointer(tuple,i,0,0);
			heap.insert(tuple_pointer);
		}
		if (schema_manager.relationExists("relation_order")){schema_manager.deleteRelation("relation_order");}
		Relation relation_order = schema_manager.createRelation("relation_order", relation_returned.getSchema());
		TuplePointer tuple_pointer_returned = heap.pop_min();
		Tuple tuple_returned = tuple_pointer_returned.tuple;
		heap.insert(tuple_pointer_returned);
		appendTupleToRelation(relation_order, mem, 9, tuple_returned);
		while(heap.size>0)
		{
			TuplePointer tuple_pointer = heap.pop_min();
			appendTupleToRelation(relation_order, mem, 9, tuple_pointer.tuple);
			if (tuple_pointer.tuple_pointer<mem.getBlock(tuple_pointer.sub_pointer).getNumTuples() - 1)
			{
				Tuple tuple = mem.getBlock(tuple_pointer.sub_pointer).getTuple(tuple_pointer.tuple_pointer + 1);
				heap.insert(new TuplePointer(tuple,tuple_pointer.sub_pointer,tuple_pointer.block_pointer,tuple_pointer.tuple_pointer + 1));
			}
			else if (tuple_pointer.block_pointer < 9 && tuple_pointer.sub_pointer * 10 + tuple_pointer.block_pointer < blocks_count - 1){
				tuple_pointer.block_pointer++;
				relation_returned.getBlock(tuple_pointer.sub_pointer *10+tuple_pointer.block_pointer, tuple_pointer.sub_pointer);
				heap.insert(new TuplePointer(mem.getBlock(tuple_pointer.sub_pointer).getTuple(0),tuple_pointer.sub_pointer,tuple_pointer.block_pointer,0));
			}
		}
		return relation_order;
	}

	private static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int block_index_mem, Tuple tuple) {
		Block block_reference;
		if (relation_reference.getNumOfBlocks() == 0) {
			block_reference=mem.getBlock(block_index_mem);
			block_reference.clear();
			block_reference.appendTuple(tuple);
			relation_reference.setBlock(relation_reference.getNumOfBlocks(),block_index_mem);
		}
		else {
			relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,block_index_mem);
			block_reference=mem.getBlock(block_index_mem);
			if (block_reference.isFull()) {
				block_reference.clear();
				block_reference.appendTuple(tuple);
				relation_reference.setBlock(relation_reference.getNumOfBlocks(),block_index_mem);
			}
			else {
				block_reference.appendTuple(tuple);
				relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,block_index_mem);
			}
		}
	}
}