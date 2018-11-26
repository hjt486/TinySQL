import java.util.ArrayList;
import java.util.regex.Pattern;
import storageManager.*;

public class Executor {
	public MainMemory mem;
	public Disk disk;
	public SchemaManager schema_manager;
	private Parser parse;

	public Executor(){
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
				case "CREATE":
					create();
					break;
				case "DROP":
					drop();
					break;
				case "INSERT":
					insert();
					if (schema_manager.relationExists("return_relation")) {
						schema_manager.deleteRelation("return_relation");
					}//drop a table
					if (schema_manager.relationExists("new_relation")) {
						schema_manager.deleteRelation("new_relation");
					}
					break;
				case "DELETE":
					delete();
					break;
				case "SELECT":
					Relation relation = select();
					System.out.println(relation);
					if (relation.getRelationName().contains(",")) {
						schema_manager.deleteRelation(relation.getRelationName());
					}
					if (schema_manager.relationExists("new_relation")) {
						schema_manager.deleteRelation("new_relation");
					}
					if (schema_manager.relationExists("return_relation")) {
						schema_manager.deleteRelation("return_relation");
					}
					if (parse.select.distinct) {
						if (schema_manager.relationExists("order_relation")) {
							schema_manager.deleteRelation("distinct_relation");
						}
					}
					if (parse.select.order) {
						if (schema_manager.relationExists("order_relation")) {
							schema_manager.deleteRelation("order_relation");
						}
					}
					if (schema_manager.relationExists("opr")) {
						schema_manager.deleteRelation("opr");
					}
					break;
				default:
					System.out.println("Input Syntax Error! Please Check");
					break;
			}
		}
		else {
			System.out.println("Input Syntax Error, Please Check!");
		}
	}

	public void create(){
		ArrayList<String> field_names = new ArrayList<>();
		ArrayList<FieldType> field_types = new ArrayList<>();
		for(int i = 0; i < parse.arg.size(); i++){
			field_names.add(parse.arg.get(i).name);
			switch (parse.arg.get(i).type.toUpperCase()){
				case "STR20":
					field_types.add(FieldType.STR20);
					break;
				case "INT":
					field_types.add(FieldType.INT);
					break;
				default:
					System.out.println("Wrong input type!");
			}
		}
		Schema schema = new Schema(field_names, field_types);
		schema_manager.createRelation(parse.table_name.get(0), schema);
		System.out.println("Table \"" + parse.table_name.get(0) + "\" is created.");
		System.out.println("Schema: ");
		System.out.println(schema);
	}

	public void drop(){
		String table = parse.table_name.get(0);
		schema_manager.deleteRelation(table);
		System.out.println("Table \"" + table + "\" is deleted.");
	}

	public void insert(){
		String table_name = parse.table_name.get(0);
		if (schema_manager.relationExists(table_name) == false){
			System.out.println("Table " + table_name + " doesn't exist!");
		}
		Relation relation = schema_manager.getRelation(table_name);
		Tuple tuple = relation.createTuple();
		Schema relation_schema = relation.getSchema();
		if (parse.select == null){
			boolean field_type_verify = false;
			for(int i = 0; i < tuple.getNumOfFields(); i++) {
				try {
					if (relation_schema.getFieldType(parse.arg.get(i).name) == FieldType.STR20) {
						String value = "";
						if (parse.values.get(i).equalsIgnoreCase("NULL")) {
							tuple.setField(parse.arg.get(i).name, value);
						} else {
							value = parse.values.get(i).replaceAll("\"", "");
							tuple.setField(parse.arg.get(i).name, value);
						}
					} else {
						if (parse.values.get(i).equalsIgnoreCase("NULL")) {
							tuple.setField(parse.arg.get(i).name, 0);
						} else {
							tuple.setField(parse.arg.get(i).name, Integer.parseInt(parse.values.get(i)));
						}
					}
					field_type_verify = true;
				}
				catch (NumberFormatException e) {
						System.out.println("Wrong format! Field " + parse.arg.get(i).name + " is of INT type!");
						System.out.println("You're trying to insert a string value.");
					field_type_verify = false;
						break;
						//tuple.setField(parse.arg.get(i).name, 0);
					}
			}
			if (field_type_verify == true)
			{
				appendTupleToRelation(relation,mem,2,tuple);
			}
		}
		else if(parse.select != null){
			Relation selected_relation = select();
			Schema schema = new Schema(selected_relation.getSchema());
			Relation relation_new = schema_manager.createRelation("new_temp", schema);
			for(int i = 0; i < selected_relation.getNumOfBlocks(); i++){
				selected_relation.getBlock(i, 9);
				relation_new.setBlock(i, 9);
			}
			int formerBlocks = relation.getNumOfBlocks();
			for(int i=0; i < relation_new.getNumOfBlocks(); i++){
				relation_new.getBlock(i,9);
				relation.setBlock(i+formerBlocks,9);
			}
		}
	}

	////////////////////////////// 11/22

	public void delete(){
		String table_name = parse.delete.t_names.get(0);
		Relation relation = schema_manager.getRelation(table_name);
		int table_blocks_count = relation.getNumOfBlocks();
		if(table_blocks_count == 0){
			System.out.println("Table \"" + table_name + "\" is empty!");
		}
		else{
			int scan_times;
			if((table_blocks_count % Config.NUM_OF_BLOCKS_IN_MEMORY) != 0){
				scan_times = table_blocks_count / Config.NUM_OF_BLOCKS_IN_MEMORY + 1;}
			else {scan_times = table_blocks_count / Config.NUM_OF_BLOCKS_IN_MEMORY;}
			for(int i = 0; i < scan_times; i++){
				int num_blocks;
				if(table_blocks_count - i * Config.NUM_OF_BLOCKS_IN_MEMORY <= Config.NUM_OF_BLOCKS_IN_MEMORY){
					num_blocks = table_blocks_count - i * Config.NUM_OF_BLOCKS_IN_MEMORY;
				}
				else{
					num_blocks = Config.NUM_OF_BLOCKS_IN_MEMORY;
				}
				relation.getBlocks(i * Config.NUM_OF_BLOCKS_IN_MEMORY, 0, num_blocks);
				ExTreeNode deleteTree = parse.delete.w_clause;
				ArrayList<Block> blocks = new ArrayList<Block>();
				for(int j = 0; j < num_blocks; j++){
					Block test_block = mem.getBlock(j);
					if(deleteTree != null) {
						ArrayList<Tuple> test_tuples = test_block.getTuples();
						if (test_tuples.size() != 0) {
							for (int k = 0; k < test_tuples.size(); k++) {
								if (deleteTree == null || where_judge(deleteTree, test_tuples.get(k))) {
									test_block.invalidateTuple(k);
								}
							}
						}
						if (test_block.getNumTuples() == 0) {
							test_block.clear();
						}
					}
					else {
						test_block.clear();
					}
					if(!test_block.isEmpty()){
						blocks.add(test_block);
					}
				}
				for(int j = 0; j < blocks.size(); j++){
					mem.setBlock(j, blocks.get(j));
				}
				relation.setBlocks(i * Config.NUM_OF_BLOCKS_IN_MEMORY, 0, num_blocks);
				relation.deleteBlocks(blocks.size());
			}
		}
	}

	public Relation select(){
		Relation joined_table;
		boolean one_pass = true;
		for(int i = 0; i < parse.select.t_names.size(); i++){
			if(schema_manager.getRelation(parse.select.t_names.get(i)).getNumOfBlocks() != 1){
				one_pass = false;
			}
		}
		if(one_pass && parse.select.t_names.size() > 1){
			System.out.println("I'm in one-pass!");
			joined_table = onepass(parse.select.t_names);
			if(parse.select.w_clause == null && parse.select.arg.get(0).equalsIgnoreCase("*")){

				if(parse.select.distinct){
					joined_table = first_pass(joined_table, joined_table.getSchema().getFieldNames());
					joined_table = distinct_second_pass(joined_table, joined_table.getSchema().getFieldNames());
				}
				if(parse.select.order == true){
					ArrayList<String> order_attr = new ArrayList<>();
					String order_by = parse.select.o_clause.trim();
					order_attr.add(order_by);
					joined_table = first_pass(joined_table, order_attr);
					if(joined_table.getNumOfBlocks() > 10){
						joined_table = order_second_pass(joined_table, order_attr);
					}
				}
				return joined_table;
			}
		}
		else{
			if(parse.select.t_names.size() == 1){
				Schema new_schema = schema_manager.getSchema(parse.select.t_names.get(0));
				joined_table = schema_manager.createRelation("new_relation", new_schema);
				int blocks = schema_manager.getRelation(parse.select.t_names.get(0)).getNumOfBlocks();
				int scan_times;
				if(blocks%9 == 0) scan_times = blocks/9;
				else scan_times = blocks/9+1;
				for(int i = 0; i < scan_times; i++){
					if((i + 1) * 9 > blocks){
						schema_manager.getRelation(parse.select.t_names.get(0)).getBlocks(i*9, 0, blocks-(9*i));
						joined_table.setBlocks(i*9, 0, blocks-(9*i));
					}
					else{
						schema_manager.getRelation(parse.select.t_names.get(0)).getBlocks(i*9, 0, 9);
						joined_table.setBlocks(i*9, 0, 9);
					}
				}
				if(parse.select.w_clause == null && parse.select.arg.get(0).equalsIgnoreCase("*")){

					if(parse.select.distinct){
						joined_table = first_pass(joined_table, joined_table.getSchema().getFieldNames());
						joined_table = distinct_second_pass(joined_table, joined_table.getSchema().getFieldNames());
					}
					if(parse.select.order){
						ArrayList<String> order_attr=new ArrayList<String>();
						String order_by =parse.select.o_clause.trim();
						order_attr.add(order_by);
						joined_table = first_pass(joined_table, order_attr);
						if(joined_table.getNumOfBlocks()>10){
							joined_table = order_second_pass(joined_table, order_attr);
						}
					}
					return joined_table;}
			}
			else{
				String pre_t = parse.select.t_names.get(0);
				String now_t;
				boolean last_one = false;
				for(int i = 1; i < parse.select.t_names.size(); i++){
					if(i == (parse.select.t_names.size()-1)) {
						last_one = true;
					}
					now_t = parse.select.t_names.get(i);
					String temp = pre_t;
					boolean na_join = false;
					if(i == 1) na_join = true;
					pre_t = new_join(pre_t,now_t,last_one,na_join);
					if(temp.contains(",")){
						schema_manager.deleteRelation(temp);
					}
				}
				joined_table = schema_manager.getRelation(pre_t);
			}
			if(parse.select.w_clause == null && parse.select.arg.get(0).equalsIgnoreCase("*")){

				if(parse.select.distinct == true){
					joined_table = first_pass(joined_table, joined_table.getSchema().getFieldNames());
					joined_table = distinct_second_pass(joined_table, joined_table.getSchema().getFieldNames());
				}
				if(parse.select.order == true){
					ArrayList<String> order_attr = new ArrayList<>();
					String order_by = parse.select.o_clause.trim();
					order_attr.add(order_by);
					joined_table = first_pass(joined_table, order_attr);
					if(joined_table.getNumOfBlocks() > 10){
						joined_table = order_second_pass(joined_table, order_attr);
					}
				}


				return joined_table;

			}
		}
		Schema return_schema;
		Relation return_relation;
		if(parse.select.t_names.size()>1){
			ArrayList<String> return_field_names = new ArrayList<String>();
			ArrayList<FieldType> return_field_types = new ArrayList<FieldType>();
			if(parse.select.arg.get(0).equalsIgnoreCase("*")){
				return_field_names = joined_table.getSchema().getFieldNames();
				return_field_types = joined_table.getSchema().getFieldTypes();
			}
			else {
				return_field_names = parse.select.arg;
				for(int i = 0; i < return_field_names.size(); i++){
					return_field_types.add(joined_table.getSchema().getFieldType(return_field_names.get(i)));
				}
			}
			return_schema = new Schema(return_field_names,return_field_types);
			return_relation = schema_manager.createRelation("return_relation", return_schema);
		}
		else {
			ArrayList<String> return_field_names = new ArrayList<>();
			ArrayList<FieldType> return_field_types = new ArrayList<>();
			if(parse.select.arg.get(0).equalsIgnoreCase("*")){
				return_field_names = joined_table.getSchema().getFieldNames();
				return_field_types = joined_table.getSchema().getFieldTypes();
			}
			else {
				return_field_names = parse.select.arg;
				for(int i = 0; i < return_field_names.size(); i++){
					if(return_field_names.get(i).split("\\.").length == 1){
						return_field_types.add(joined_table.getSchema().getFieldType(return_field_names.get(i)));
					}
					else{
						String real_name = return_field_names.get(i).split("\\.")[1];
						return_field_types.add(joined_table.getSchema().getFieldType(real_name));
					}
				}
			}
			return_schema = new Schema(return_field_names,return_field_types);
			return_relation = schema_manager.createRelation("return_relation", return_schema);
		}
//		   System.out.println(joined_table);
		int t_blocks = joined_table.getNumOfBlocks();
		if(t_blocks == 0){
			System.out.println("The table is an empty table!");
		}



		int scan_times;
		if((t_blocks%(Config.NUM_OF_BLOCKS_IN_MEMORY-1))!=0){
			scan_times = t_blocks/(Config.NUM_OF_BLOCKS_IN_MEMORY-1)+1;}
		else {scan_times = t_blocks/(Config.NUM_OF_BLOCKS_IN_MEMORY-1);}
		for(int i = 0; i < scan_times; i++){
			int num_blocks;
			if(t_blocks-i*(Config.NUM_OF_BLOCKS_IN_MEMORY-1)<(Config.NUM_OF_BLOCKS_IN_MEMORY-1)){
				num_blocks = t_blocks-(i*(Config.NUM_OF_BLOCKS_IN_MEMORY-1));
			}
			else{
				num_blocks = Config.NUM_OF_BLOCKS_IN_MEMORY-1;
			}
			joined_table.getBlocks(i*(Config.NUM_OF_BLOCKS_IN_MEMORY-1), 0, num_blocks);
			ExTreeNode selectTree = parse.select.w_clause;
			for(int j = 0; j < num_blocks; j++){
				Block test_block = mem.getBlock(j);
				ArrayList<Tuple> test_tuples = test_block.getTuples();
				if(test_tuples.size() == 0) continue;
				for(int k=0; k < test_tuples.size(); k++){
					if(selectTree == null || where_judge(selectTree,test_tuples.get(k))){
						if(parse.select.arg.get(0).equalsIgnoreCase("*")){
//								System.out.println(test_tuples.get(k));
							appendTupleToRelation(return_relation,mem,9,test_tuples.get(k));
						}
						else{
							Tuple return_tuple = return_relation.createTuple();
							for(int n = 0; n < parse.select.arg.size(); n++){
								if(parse.select.t_names.size() == 1){
									String[] table_attr = parse.select.arg.get(n).split("\\.");
									if(table_attr.length == 1){
										if(return_schema.getFieldType(parse.select.arg.get(n)) == FieldType.STR20){
											return_tuple.setField(parse.select.arg.get(n),test_block.getTuple(k).getField(parse.select.arg.get(n)).str);}
										else{
											return_tuple.setField(parse.select.arg.get(n),test_block.getTuple(k).getField(parse.select.arg.get(n)).integer);}
									}
									else{
										if(return_schema.getFieldType(parse.select.arg.get(n))==FieldType.STR20){
											return_tuple.setField(parse.select.arg.get(n),test_block.getTuple(k).getField(table_attr[1]).str);}
										else{
											return_tuple.setField(parse.select.arg.get(n),test_block.getTuple(k).getField(table_attr[1]).integer);}
									}
								}
								else{
									if(return_schema.getFieldType(parse.select.arg.get(n)) == FieldType.STR20){
										return_tuple.setField(parse.select.arg.get(n),test_block.getTuple(k).getField(parse.select.arg.get(n)).str);}
									else{
										return_tuple.setField(parse.select.arg.get(n),test_block.getTuple(k).getField(parse.select.arg.get(n)).integer);}
								}
							}
							appendTupleToRelation(return_relation,mem,9,return_tuple);
						}
					}
				}
			}

//			System.out.println("test");
		}
		if(parse.select.distinct){
			return_relation = first_pass(return_relation, return_relation.getSchema().getFieldNames());
			return_relation = distinct_second_pass(return_relation, return_relation.getSchema().getFieldNames());
		}
		if(parse.select.order){
			ArrayList<String> order_attr = new ArrayList<>();
			String order_by = parse.select.o_clause.trim();
			order_attr.add(order_by);
			return_relation = first_pass(return_relation, order_attr);
			if(return_relation.getNumOfBlocks() > 10){
				return_relation=order_second_pass(return_relation, order_attr);
			}
		}
		if(parse.select.t_names.size()>1) schema_manager.deleteRelation(joined_table.getRelationName());

		return return_relation;
	}

	private Relation onepass(ArrayList<String> t_names){
		Schema one_pass_schema = merge_schema(t_names);
		Relation opr = schema_manager.createRelation("opr", one_pass_schema);
		for(int j = 0; j < t_names.size(); j++){
			schema_manager.getRelation(t_names.get(j)).getBlock(0, j);
		}
		ArrayList<Tuple> tuples = new ArrayList<>();
		int nums = 1;
		for(int i = 0; i < t_names.size(); i++){
			nums = nums*(schema_manager.getRelation(t_names.get(i)).getNumOfTuples());
		}
		for(int i = 0; i<nums; i++){
			Tuple t_temp = opr.createTuple();
			tuples.add(t_temp);
		}

		if(!parse.select.distinct && !parse.select.order && parse.select.w_clause == null && parse.select.arg.get(0).equalsIgnoreCase("*")){
			System.out.println(one_pass_schema);
		}
		tuples=one_pass_mem(tuples,t_names.size(),0,t_names,nums);
		for(int i = 0; i < tuples.size(); i++){
			if(!parse.select.distinct && !parse.select.order && parse.select.w_clause == null && parse.select.arg.get(0).equalsIgnoreCase("*")){
				System.out.println(tuples.get(i));
			}
			else{
				appendTupleToRelation(opr, mem,9, tuples.get(i));
			}
		}
		return opr;
	}

	private ArrayList<Tuple> one_pass_mem(ArrayList<Tuple> tuples,int times, int current_time, ArrayList<String> t_names, int total_t_nums){
		if(current_time == (times-1)){
			int rec_times = schema_manager.getRelation(t_names.get(current_time)).getNumOfTuples();
			int every_rec = total_t_nums/rec_times;
			int total_f_nums = tuples.get(0).getNumOfFields();


			for(int i = 0; i < rec_times; i++){
				for(int j = i * every_rec; j < (every_rec*(i+1)); j++){
					int field_nums = schema_manager.getSchema(t_names.get(current_time)).getNumOfFields();
					for(int k = 0; k < field_nums; k++){
						if(tuples.get(j).getField(total_f_nums-1-k).type == FieldType.STR20){
							tuples.get(j).setField(total_f_nums-1-k,mem.getBlock(current_time).getTuple(i).getField(field_nums-k-1).str);}
						else{
							tuples.get(j).setField(total_f_nums-1-k,mem.getBlock(current_time).getTuple(i).getField(field_nums-k-1).integer);}
					}
				}
			}
			return tuples;
		}


		tuples = one_pass_mem(tuples,times,current_time+1,t_names,total_t_nums);
		int rec_times = schema_manager.getRelation(t_names.get(current_time)).getNumOfTuples();
		int every_rec = total_t_nums/rec_times;
		int total_f_nums = tuples.get(0).getNumOfFields();
		int pre_f_nums=0;


		for(int i = 0; i < current_time; i++){
			pre_f_nums = schema_manager.getSchema(t_names.get(i)).getNumOfFields() + pre_f_nums;
		}
		for(int i = 0; i < rec_times; i++){
			for(int j = i * every_rec; j < (every_rec*(i+1)); j++){
				int field_nums = schema_manager.getSchema(t_names.get(current_time)).getNumOfFields();
				for(int k=0; k < field_nums; k++){
					if(tuples.get(j).getField(pre_f_nums+k).type == FieldType.STR20){
						tuples.get(j).setField(pre_f_nums+k, mem.getBlock(current_time).getTuple(i).getField(k).str);}
					else{
						tuples.get(j).setField(pre_f_nums+k,mem.getBlock(current_time).getTuple(i).getField(k).integer);}
				}
			}
		}
		return tuples;
	}


	private Schema merge_schema(ArrayList<String> t_names){
		Schema[] schemas = new Schema[t_names.size()];
		ArrayList<String> joined_fieldnames = new ArrayList<>();
		ArrayList<FieldType> joined_fieldtypes = new ArrayList<>();
		for(int i = 0; i < schemas.length; i++){
			schemas[i] = schema_manager.getSchema(t_names.get(i));
			ArrayList<String> field_names = schemas[i].getFieldNames();
			for(int j = 0; j < field_names.size(); j++){
				String new_name = t_names.get(i)+"."+field_names.get(j);
				field_names.set(j, new_name);
			}
			joined_fieldnames.addAll(field_names);
			joined_fieldtypes.addAll(schemas[i].getFieldTypes());
		}
		Schema joined_schema = new Schema(joined_fieldnames,joined_fieldtypes);
		return joined_schema;
	}


	private String natural_join(String t_1, String t_2, String attr){
		Relation r_1 = schema_manager.getRelation(t_1);
		Relation r_2 = schema_manager.getRelation(t_2);
		ArrayList<FieldType> fieldtypes = r_1.getSchema().getFieldTypes();
		ArrayList<FieldType> fieldtypes2 = r_2.getSchema().getFieldTypes();
		fieldtypes.addAll(fieldtypes2);
		ArrayList<String> new_fieldnames = new ArrayList<String>();
		if(r_1.getSchema().getFieldNames().get(0).contains(".")){
			new_fieldnames = r_1.getSchema().getFieldNames();
		}
		else{
			for(int i=0; i<r_1.getSchema().getNumOfFields(); i++){
				String name = t_1+"."+r_1.getSchema().getFieldNames().get(i);
				new_fieldnames.add(name);
			}
		}
		if(r_2.getSchema().getFieldNames().get(0).contains(".")){
			new_fieldnames.addAll(r_2.getSchema().getFieldNames());
		}
		else{
			for(int i = 0; i < r_2.getSchema().getNumOfFields(); i++){
				String name = t_2+"."+r_2.getSchema().getFieldNames().get(i);
				new_fieldnames.add(name);
			}
		}
		String n_j_name = t_1+","+t_2;
		Schema n_j_schema = new Schema(new_fieldnames,fieldtypes);
		Relation njr = schema_manager.createRelation(n_j_name, n_j_schema);
		ArrayList<String> n_j_field = new ArrayList<>();
		n_j_field.add(attr);
		//first pass
		r_1 = first_pass(r_1,n_j_field);
		r_2 = first_pass(r_2,n_j_field);
		//second pass
		Heap heap1 = new Heap(80,n_j_field);
		Heap heap2 = new Heap(80,n_j_field);
		int r_1_blocks = r_1.getNumOfBlocks();
		int r_2_blocks = r_2.getNumOfBlocks();
		int sb_num1 = 0;
		if(r_1_blocks%Config.NUM_OF_BLOCKS_IN_MEMORY == 0){
			sb_num1 = r_1_blocks/Config.NUM_OF_BLOCKS_IN_MEMORY;
		}
		else{
			sb_num1 = r_1_blocks/Config.NUM_OF_BLOCKS_IN_MEMORY+1;
		}
		int sb_num2 = 0;
		if(r_2_blocks%Config.NUM_OF_BLOCKS_IN_MEMORY == 0){
			sb_num2 = r_2_blocks / Config.NUM_OF_BLOCKS_IN_MEMORY;
		}
		else{
			sb_num2 = r_2_blocks / Config.NUM_OF_BLOCKS_IN_MEMORY+1;
		}

		for(int i = 0; i < sb_num1; i++){//put the first block of each sublist of relation1 into memory, starting from 0
			r_1.getBlock(i*Config.NUM_OF_BLOCKS_IN_MEMORY, i);
			Tuple_with_position tuple_with_p = new Tuple_with_position(mem.getBlock(i).getTuple(0),i,0,0);
			heap1.insert(tuple_with_p);
		}
		for(int i = 0; i < sb_num2; i++){//put the first block of each sublist of relation2 into memory, starting from sublistNum1
			r_2.getBlock(i*Config.NUM_OF_BLOCKS_IN_MEMORY, i+sb_num1);
			Tuple_with_position tuple_with_p = new Tuple_with_position(mem.getBlock(i+sb_num1).getTuple(0),i+sb_num1,0,0);
			heap2.insert(tuple_with_p);
		}
		while(heap1.size > 0 && heap2.size > 0){
			Tuple_with_position t_p_1 = heap1.pop_min();
			Tuple_with_position t_p_2 = heap2.pop_min();
			heap1.insert(t_p_1);
			heap2.insert(t_p_2);

			if(t_p_1.tuple.getField(attr).integer>t_p_2.tuple.getField(attr).integer){
				Tuple_with_position t_p_temp = heap2.pop_min();
				if(t_p_temp.tuple_pointer < mem.getBlock(t_p_temp.sublist_pointer).getNumTuples()-1){
					Tuple tuple = mem.getBlock(t_p_temp.sublist_pointer).getTuple(t_p_temp.tuple_pointer+1);
					heap2.insert(new Tuple_with_position(tuple,t_p_temp.sublist_pointer,t_p_temp.block_pointer,t_p_temp.tuple_pointer+1));
				}
				else if(t_p_temp.block_pointer < 9 && (t_p_temp.sublist_pointer-sb_num1)*10+t_p_temp.block_pointer<r_2_blocks-1){
					t_p_temp.block_pointer++;
					r_2.getBlock((t_p_temp.sublist_pointer-sb_num1)*10+t_p_temp.block_pointer,t_p_temp.sublist_pointer);
					heap2.insert(new Tuple_with_position(mem.getBlock(t_p_temp.sublist_pointer).getTuple(0),t_p_temp.sublist_pointer,t_p_temp.block_pointer,0));
				}
			}
			else if(t_p_1.tuple.getField(attr).integer<t_p_2.tuple.getField(attr).integer){
				Tuple_with_position t_p_temp = heap1.pop_min();
				if(t_p_temp.tuple_pointer < mem.getBlock(t_p_temp.sublist_pointer).getNumTuples() - 1){
					Tuple tuple = mem.getBlock(t_p_temp.sublist_pointer).getTuple(t_p_temp.tuple_pointer+1);
					heap1.insert(new Tuple_with_position(tuple,t_p_temp.sublist_pointer,t_p_temp.block_pointer,t_p_temp.tuple_pointer+1));
				}
				else if(t_p_temp.block_pointer<9 && t_p_temp.sublist_pointer*10+t_p_temp.block_pointer<r_1_blocks-1){
					t_p_temp.block_pointer++;
					r_1.getBlock(t_p_temp.sublist_pointer*10+t_p_temp.block_pointer,t_p_temp.sublist_pointer);
					heap1.insert(new Tuple_with_position(mem.getBlock(t_p_temp.sublist_pointer).getTuple(0),t_p_temp.sublist_pointer,t_p_temp.block_pointer,0));
				}
			}
			else{
				Tuple_with_position t_p_1_temp = heap1.pop_min();
				Tuple_with_position t_p_2_temp = heap2.pop_min();
				if(t_p_2_temp.tuple_pointer < mem.getBlock(t_p_2_temp.sublist_pointer).getNumTuples()-1){
					Tuple tuple = mem.getBlock(t_p_2_temp.sublist_pointer).getTuple(t_p_2_temp.tuple_pointer+1);
					heap2.insert(new Tuple_with_position(tuple,t_p_2_temp.sublist_pointer,t_p_2_temp.block_pointer,t_p_2_temp.tuple_pointer+1));
				}
				else if(t_p_2_temp.block_pointer<9 && (t_p_2_temp.sublist_pointer-sb_num1)*10+t_p_2_temp.block_pointer<r_2_blocks-1){
					t_p_2_temp.block_pointer++;
					r_2.getBlock((t_p_2_temp.sublist_pointer-sb_num1)*10+t_p_2_temp.block_pointer,t_p_2_temp.sublist_pointer);
					heap2.insert(new Tuple_with_position(mem.getBlock(t_p_2_temp.sublist_pointer).getTuple(0),t_p_2_temp.sublist_pointer,t_p_2_temp.block_pointer,0));
				}
				if(t_p_1_temp.tuple_pointer<mem.getBlock(t_p_1_temp.sublist_pointer).getNumTuples()-1){
					Tuple tuple = mem.getBlock(t_p_1_temp.sublist_pointer).getTuple(t_p_1_temp.tuple_pointer+1);
					heap1.insert(new Tuple_with_position(tuple,t_p_1_temp.sublist_pointer,t_p_1_temp.block_pointer,t_p_1_temp.tuple_pointer+1));
				}
				else if(t_p_1_temp.block_pointer<9 && t_p_1_temp.sublist_pointer*10+t_p_1_temp.block_pointer<r_1_blocks-1){
					t_p_1_temp.block_pointer++;
					r_1.getBlock(t_p_1_temp.sublist_pointer*10+t_p_1_temp.block_pointer,t_p_1_temp.sublist_pointer);
					heap1.insert(new Tuple_with_position(mem.getBlock(t_p_1_temp.sublist_pointer).getTuple(0),t_p_1_temp.sublist_pointer,t_p_1_temp.block_pointer,0));
				}
				Tuple tuple1 = t_p_1_temp.tuple;
				Tuple tuple2 = t_p_2_temp.tuple;
				Tuple njt = njr.createTuple();
				int t1_fields = tuple1.getNumOfFields();
				for(int k = 0; k < t1_fields; k++){
					if(tuple1.getField(k).type == FieldType.INT){
						njt.setField(k,tuple1.getField(k).integer);
					}
					else{
						njt.setField(k,tuple1.getField(k).str);
					}
				}
				for(int n = 0; n < tuple2.getNumOfFields(); n++){
					if(tuple2.getField(n).type == FieldType.INT){
						njt.setField(n+t1_fields,tuple2.getField(n).integer);
					}
					else{
						njt.setField(n+t1_fields,tuple2.getField(n).str);
					}
				}
				appendTupleToRelation(njr,mem,9,njt);
				Tuple_with_position pop1 = heap1.pop_min();
				Tuple_with_position pop2 = heap2.pop_min();
				Tuple compare_1 = pop1.tuple;
				Tuple compare_2 = pop2.tuple;
				heap1.insert(pop1);
				heap2.insert(pop2);
				while(heap1.size > 0 && heap1.compare_tuple(tuple1, compare_1) == 0){
					Tuple_with_position new_tp1 = heap1.pop_min();
					//process newTuplePlus1,tuplePlus2;
					Tuple new_tuple1 = new_tp1.tuple;
					t1_fields = new_tuple1.getNumOfFields();
					Tuple new_njt = njr.createTuple();
					for(int k = 0; k < t1_fields; k++){
						if(new_tuple1.getField(k).type == FieldType.INT){
							new_njt.setField(k,new_tuple1.getField(k).integer);
						}
						else{
							new_njt.setField(k,new_tuple1.getField(k).str);
						}
					}
					for(int n = 0; n < tuple2.getNumOfFields(); n++){
						if(tuple2.getField(n).type == FieldType.INT){
							new_njt.setField(n+t1_fields,tuple2.getField(n).integer);
						}
						else{
							new_njt.setField(n+t1_fields,tuple2.getField(n).str);
						}
					}
					appendTupleToRelation(njr, mem,9, new_njt);


					Tuple_with_position t_p_temp = new_tp1;
					if(t_p_temp.tuple_pointer<mem.getBlock(t_p_temp.sublist_pointer).getNumTuples()-1){
						Tuple tuple=mem.getBlock(t_p_temp.sublist_pointer).getTuple(t_p_temp.tuple_pointer+1);
						heap1.insert(new Tuple_with_position(tuple,t_p_temp.sublist_pointer,t_p_temp.block_pointer,t_p_temp.tuple_pointer+1));
					}
					else if(t_p_temp.block_pointer<9 && t_p_temp.sublist_pointer*10+t_p_temp.block_pointer<r_1_blocks-1){
						t_p_temp.block_pointer++;
						r_1.getBlock(t_p_temp.sublist_pointer*10+t_p_temp.block_pointer,t_p_temp.sublist_pointer);
						heap1.insert(new Tuple_with_position(mem.getBlock(t_p_temp.sublist_pointer).getTuple(0),t_p_temp.sublist_pointer,t_p_temp.block_pointer,0));
					}
					if(heap1.size > 0){
						Tuple_with_position temp1=heap1.pop_min();
						heap1.insert(temp1);
						compare_1=temp1.tuple;}

				}

				while(heap2.size>0 && heap2.compare_tuple(tuple2, compare_2)==0){
					Tuple_with_position new_tp2 = heap2.pop_min();
					//process newTuplePlus1,tuplePlus2;
					Tuple new_tuple2=new_tp2.tuple;
					Tuple new_njt=njr.createTuple();
					for(int k=0;k<t1_fields;k++){
						if(tuple1.getField(k).type == FieldType.INT){
							new_njt.setField(k,tuple1.getField(k).integer);
						}
						else{
							new_njt.setField(k,tuple1.getField(k).str);
						}
					}
					for(int n=0;n<new_tuple2.getNumOfFields();n++){
						if(new_tuple2.getField(n).type == FieldType.INT){
							new_njt.setField(n+t1_fields,new_tuple2.getField(n).integer);
						}
						else{
							new_njt.setField(n+t1_fields,new_tuple2.getField(n).str);
						}
					}
					appendTupleToRelation(njr,mem,9,new_njt);


					Tuple_with_position t_p_temp=new_tp2;
					if(t_p_temp.tuple_pointer<mem.getBlock(t_p_temp.sublist_pointer).getNumTuples()-1){
						Tuple tuple=mem.getBlock(t_p_temp.sublist_pointer).getTuple(t_p_temp.tuple_pointer+1);
						heap2.insert(new Tuple_with_position(tuple,t_p_temp.sublist_pointer,t_p_temp.block_pointer,t_p_temp.tuple_pointer+1));
					}
					else if(t_p_temp.block_pointer<9 && (t_p_temp.sublist_pointer-sb_num1)*10+t_p_temp.block_pointer<r_2_blocks-1){
						t_p_temp.block_pointer++;
						r_2.getBlock((t_p_temp.sublist_pointer-sb_num1)*10+t_p_temp.block_pointer,t_p_temp.sublist_pointer);
						heap2.insert(new Tuple_with_position(mem.getBlock(t_p_temp.sublist_pointer).getTuple(0),t_p_temp.sublist_pointer,t_p_temp.block_pointer,0));
					}
					if(heap2.size>0){
						Tuple_with_position temp2=heap2.pop_min();
						heap2.insert(temp2);
						compare_2=temp2.tuple;
					}


				}

			}
		}
		return n_j_name;

	}




	private String new_join(String t_1, String t_2, boolean last_one, boolean na_join){
		long r=System.currentTimeMillis();
		ArrayList<ExTreeNode> clauses=new ArrayList<ExTreeNode>();
		if(parse.select.w_clause!=null) clauses=parse.select.w_clause.hasSelection();
		ArrayList<ExTreeNode> suit_clauses=new ArrayList<ExTreeNode>();
		ArrayList<String> t1_names=new ArrayList<String>();
		if(t_1.contains(","))
		{
			String[] t1_names_s=t_1.split(",");
			for(int i=0;i<t1_names_s.length;i++){
				t1_names.add(t1_names_s[i]);
			}
		}
		for(int i=0;i<clauses.size();i++){
			ExTreeNode test_tree=clauses.get(i);
			boolean add=false;
			if(t1_names.size()>0){
				for(int j=0;j<t1_names.size();j++){
					if((test_tree.left.op.contains(t1_names.get(j))&&test_tree.right.op.contains(t_2))){
						add=true;
					}
				}
			}
			else{
				if(test_tree.left.op.contains(t_1)&&test_tree.right.op.contains(t_2)){
					if(test_tree.left.op.contains(t_1)&&test_tree.right.op.contains(t_2)){
						String left_op = test_tree.left.op;
						String right_op = test_tree.right.op;
						int index_left = left_op.indexOf(".");
						int index_right = right_op.indexOf(".");
						if(index_left>0 && index_right>0){
							String sub_left = left_op.substring(index_left+1,left_op.length());
							String sub_right = right_op.substring(index_right+1,right_op.length());
							if(sub_left.equalsIgnoreCase(sub_right) && na_join){
//	                            to do natural join
//	                            tables t_1 && t_2
//	                            join attribute
//	                            System.out.println("I'm in natural join");
//	                            sub_left.replaceAll("\.", "");
								return natural_join(t_1,t_2,sub_left);
							}
						}
						add=true;
					}
				}
			}

			if(add==true){
				suit_clauses.add(test_tree);}
		}
		Relation r_1=schema_manager.getRelation(t_1);
		Relation r_2=schema_manager.getRelation(t_2);
		ArrayList<String> new_fieldnames=new ArrayList<String>();
		ArrayList<FieldType> new_fieldtypes=r_1.getSchema().getFieldTypes();
		new_fieldtypes.addAll(r_2.getSchema().getFieldTypes());
		if(r_1.getSchema().getFieldNames().get(0).contains(".")){
			new_fieldnames=r_1.getSchema().getFieldNames();
		}
		else{
			for(int i=0;i<r_1.getSchema().getNumOfFields();i++){
				String name=t_1+"."+r_1.getSchema().getFieldNames().get(i);
				new_fieldnames.add(name);
			}
		}
		if(r_2.getSchema().getFieldNames().get(0).contains(".")){
			new_fieldnames.addAll(r_2.getSchema().getFieldNames());
		}
		else{
			for(int i=0;i<r_2.getSchema().getNumOfFields();i++){
				String name=t_2+"."+r_2.getSchema().getFieldNames().get(i);
				new_fieldnames.add(name);
			}
		}
		Schema new_schema=new Schema(new_fieldnames,new_fieldtypes);
		if(last_one) System.out.println(new_schema);
		String new_table=t_1+","+t_2;
		Relation new_r =schema_manager.createRelation(new_table, new_schema);
		int t1_blocks=r_1.getNumOfBlocks();
		int t1_to_mem;
		if(t1_blocks>Config.NUM_OF_BLOCKS_IN_MEMORY-2) t1_to_mem=Config.NUM_OF_BLOCKS_IN_MEMORY-2;
		else t1_to_mem=t1_blocks;
		for(int i=0;i<t1_blocks;i+=t1_to_mem){
			if(i+t1_to_mem>t1_blocks) t1_to_mem=t1_blocks-i;
			r_1.getBlocks(i, 0, t1_to_mem);
			for(int j=0;j<t1_to_mem;j++){
				long lp=System.currentTimeMillis();
				Block t1_block=mem.getBlock(j);
				if(t1_block.isEmpty()) continue;
				int t2_blocks=r_2.getNumOfBlocks();
				for(int m=0;m<t2_blocks;m++){
					r_2.getBlock(m, Config.NUM_OF_BLOCKS_IN_MEMORY-2);
					Block t2_block=mem.getBlock(Config.NUM_OF_BLOCKS_IN_MEMORY-2);
					if(t2_block.isEmpty()) continue;
					for(int k=0;k<t1_block.getNumTuples();k++){
						Tuple t1_tuple=t1_block.getTuple(k);
						if(t1_tuple.isNull()) continue;
						for(int n=0;n<t2_block.getNumTuples();n++){
							Tuple t2_tuple=t2_block.getTuple(n);
							if(t2_tuple.isNull()) continue;


							Tuple joined_tuple=new_r.createTuple();
							for(int t=0;t<t1_tuple.getNumOfFields();t++){
								if(t1_tuple.getField(t).type==FieldType.INT){
									joined_tuple.setField(t, t1_tuple.getField(t).integer);
								}
								else if(t1_tuple.getField(t).type==FieldType.STR20){
									joined_tuple.setField(t, t1_tuple.getField(t).str);
								}
							}
							for(int t=t1_tuple.getNumOfFields();t<joined_tuple.getNumOfFields();t++){
								if(t2_tuple.getField(t-t1_tuple.getNumOfFields()).type==FieldType.INT){
									joined_tuple.setField(t, t2_tuple.getField(t-t1_tuple.getNumOfFields()).integer);
								}
								else if(t2_tuple.getField(t-t1_tuple.getNumOfFields()).type==FieldType.STR20){
									joined_tuple.setField(t, t2_tuple.getField(t-t1_tuple.getNumOfFields()).str);
								}
							}
							if(suit_clauses.size()==0) {
								appendTupleToRelation(new_r, mem, Config.NUM_OF_BLOCKS_IN_MEMORY-1, joined_tuple);
							}
							else{
								int pointer=0;
								for(int t=0;t<suit_clauses.size();t++){
									if(where_judge(suit_clauses.get(t),joined_tuple)){
										pointer++;
									}
								}
								if(pointer==suit_clauses.size()){
									appendTupleToRelation(new_r, mem, Config.NUM_OF_BLOCKS_IN_MEMORY-1, joined_tuple);
								}
							}
						}
					}
				}
			}
		}
		return new_table;
	}

	private boolean where_judge(ExTreeNode ExTree, Tuple test_tuple){
		if(ExTree==null) return true;
		if(calculate(ExTree, test_tuple).equalsIgnoreCase("true")) return true;
		else if(calculate(ExTree, test_tuple).equalsIgnoreCase("null")) System.out.println("Syntax Error!");
		return false;
	}

	private String calculate(ExTreeNode ExTree, Tuple test_tuple){
		if(ExTree.left == null){
			return ExTree.op;
		}
		if(ExTree.right ==null){
			return ExTree.op;
		}
		String left = "false";
		String right = "false";

		if(ExTree.left != null){
			left=calculate(ExTree.left, test_tuple);
		}
		if(ExTree.right != null){
			right=calculate(ExTree.right,test_tuple);
		}
		if(ExTree.op.equalsIgnoreCase("&")){
			if(left.equalsIgnoreCase("true")&&right.equalsIgnoreCase("true")){
				return "true";
			}
			else return "false";
		}
		else if(ExTree.op.equalsIgnoreCase("|")){
			if(left.equalsIgnoreCase("true")||right.equalsIgnoreCase("true")){
				return "true";
			}
			else return "false";
		}
		else if(ExTree.op.equalsIgnoreCase("=")){
			if(Pattern.matches("[0-9]",String.valueOf(left.charAt(0)))){
				if(Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if(left.equalsIgnoreCase(right)) return "true";
					else return "false";}
				else if(Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					int lvalue=Integer.parseInt(left);
					int rvalue=test_tuple.getField(right).integer;
					if(lvalue==rvalue) return "true";
					else return "false";
				}
			}
			else if(Pattern.matches("[^0-9]",String.valueOf(left.charAt(0)))){
				if(Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					int rvalue=Integer.parseInt(right);
					int lvalue=test_tuple.getField(left).integer;
					if(lvalue==rvalue) return "true";
					else return "false";
				}
				else if(Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if(test_tuple.getSchema().fieldNameExists(right)) {
						if(test_tuple.getSchema().fieldNameExists(left)){
							if(test_tuple.getField(left).str!=null){
								if(test_tuple.getField(right).str.equalsIgnoreCase(test_tuple.getField(left).str)) return "true";
								else return "false";
							}
							else{
								if(test_tuple.getField(right).integer==test_tuple.getField(left).integer) return "true";
								else return "false";
							}
						}
						else{
							left=left.replaceAll("\"", "");
							if(test_tuple.getField(right).str.equalsIgnoreCase(left)) return "true";
							else return "false";
						}
					}
					else if(!test_tuple.getSchema().fieldNameExists(right)){
						if(test_tuple.getSchema().fieldNameExists(left)){
							right=right.replaceAll("\"", "");
							if(test_tuple.getField(left).str.equalsIgnoreCase(right)) return "true";
							else return "false";
						}
						else{
							if(left.equalsIgnoreCase(right)) return "true";
							else return "false";
						}
					}
				}
			}
		}
		else if(ExTree.op.equalsIgnoreCase("<")){
			if(Pattern.matches("[^0-9]", String.valueOf(left.charAt(0)))){
				if(Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if(test_tuple.getField(left).integer<test_tuple.getField(right).integer) return "true";
					else return "false";
				}
				else if(Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if(test_tuple.getField(left).integer<Integer.parseInt(right)) return "true";
					else return "false";
				}
			}
			else if(Pattern.matches("[0-9]", String.valueOf(left.charAt(0)))){
				if(Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if(Integer.parseInt(left)<test_tuple.getField(right).integer) return "true";
					else return "false";
				}
				else if(Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if(Integer.parseInt(left)<Integer.parseInt(right)) return "true";
					else return "false";
				}
			}
		}
		else if(ExTree.op.equalsIgnoreCase(">")){
			if(Pattern.matches("[^0-9]", String.valueOf(left.charAt(0)))){
				if(Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if(test_tuple.getField(left).integer>test_tuple.getField(right).integer) return "true";
					else return "false";
				}
				else if(Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if(test_tuple.getField(left).integer>Integer.parseInt(right)) return "true";
					else return "false";
				}
			}
			else if(Pattern.matches("[0-9]", String.valueOf(left.charAt(0)))){
				if(Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					if(Integer.parseInt(left)>test_tuple.getField(right).integer) return "true";
					else return "false";
				}
				else if(Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					if(Integer.parseInt(left)>Integer.parseInt(right)) return "true";
					else return "false";
				}
			}
		}
		else if(ExTree.op.equalsIgnoreCase("+")  || ExTree.op.equalsIgnoreCase("-")  || ExTree.op.equalsIgnoreCase("*")  || ExTree.op.equalsIgnoreCase("/")){
			if(Pattern.matches("[0-9]",String.valueOf(left.charAt(0)))){
				if(Pattern.matches("[0-9]", String.valueOf(right.charAt(0)))){
					int temp=0;
					if(ExTree.op.equalsIgnoreCase("+")){
						temp=Integer.parseInt(left)+Integer.parseInt(right);}
					else if(ExTree.op.equalsIgnoreCase("-")){
						temp=Integer.parseInt(left)-Integer.parseInt(right);}
					else if(ExTree.op.equalsIgnoreCase("*")){
						temp=Integer.parseInt(left)*Integer.parseInt(right);}
					else if(ExTree.op.equalsIgnoreCase("/")){
						temp=Integer.parseInt(left)/Integer.parseInt(right);}
					return String.valueOf(temp);	             //如果是左边的是数字，那右边也得是数字或者字符
				}
				else if(Pattern.matches("[^0-9]", String.valueOf(right.charAt(0)))){
					int right_value=test_tuple.getField(right).integer;
					int temp=0;
					if(ExTree.op.equalsIgnoreCase("+")){
						temp=Integer.parseInt(left)+right_value;}
					else if(ExTree.op.equalsIgnoreCase("-")){
						temp=Integer.parseInt(left)-right_value;}
					else if(ExTree.op.equalsIgnoreCase("*")){
						temp=Integer.parseInt(left)*right_value;}
					else if(ExTree.op.equalsIgnoreCase("/")){
						temp=Integer.parseInt(left)/right_value;}
					return String.valueOf(temp);
				}
			}
			else if(Pattern.matches("[0-9]",String.valueOf(right.charAt(0)))){
				if(Pattern.matches("[^0-9]", String.valueOf(left.charAt(0)))){
					int left_value=test_tuple.getField(left).integer;
					int temp=0;
					if(ExTree.op.equalsIgnoreCase("+")){
						temp=Integer.parseInt(right)+left_value;}
					else if(ExTree.op.equalsIgnoreCase("-")){
						temp=Integer.parseInt(right)-left_value;}
					else if(ExTree.op.equalsIgnoreCase("*")){
						temp=Integer.parseInt(right)*left_value;}
					else if(ExTree.op.equalsIgnoreCase("/")){
						temp=Integer.parseInt(right)/left_value;}
					return String.valueOf(temp);
				}
			}
			else if(Pattern.matches("[^0-9]",String.valueOf(left.charAt(0)))&&Pattern.matches("[^0-9]",String.valueOf(right.charAt(0)))){
				int left_value=test_tuple.getField(left).integer;
				int right_value=test_tuple.getField(right).integer;
				int temp=0;
				if(ExTree.op.equalsIgnoreCase("+")){
					temp=right_value+left_value;}
				else if(ExTree.op.equalsIgnoreCase("-")){
					temp=right_value-left_value;}
				else if(ExTree.op.equalsIgnoreCase("*")){
					temp=right_value*left_value;}
				else if(ExTree.op.equalsIgnoreCase("/")){
					temp=right_value/left_value;}
				return String.valueOf(temp);
			}

		}
		return "null";
	}

	private Relation first_pass(Relation return_relation, ArrayList<String> field_names){
//		 if(return_relation.getSchema().getFieldNames().contains(field_names.get(0))){
//			 return return_relation;
//		 }
		Heap heap=new Heap(80,field_names);
		int num_blocks=return_relation.getNumOfBlocks();
		int scan_times=0;
		if(num_blocks%Config.NUM_OF_BLOCKS_IN_MEMORY==0)  scan_times=num_blocks/Config.NUM_OF_BLOCKS_IN_MEMORY;
		else scan_times=num_blocks/Config.NUM_OF_BLOCKS_IN_MEMORY+1;

		for(int i=0;i<scan_times;i++){
			if(num_blocks-Config.NUM_OF_BLOCKS_IN_MEMORY*i>=Config.NUM_OF_BLOCKS_IN_MEMORY){

				return_relation.getBlocks(i*Config.NUM_OF_BLOCKS_IN_MEMORY, 0, Config.NUM_OF_BLOCKS_IN_MEMORY);
				for(int j=0;j<Config.NUM_OF_BLOCKS_IN_MEMORY;j++){
					Block sort_block=mem.getBlock(j);
					if(sort_block.isEmpty()) {continue;}
					int d=sort_block.getNumTuples();
					for(int k=0;k<sort_block.getNumTuples();k++){
						Tuple sort_tuple=sort_block.getTuple(k);
						if(sort_tuple.isNull())  {continue;}
						Tuple_with_position tp=new Tuple_with_position(sort_tuple,0,0,0);
						heap.insert(tp);
					}
				}
				for(int j=0;j<Config.NUM_OF_BLOCKS_IN_MEMORY;j++){
					Block sorted_block=mem.getBlock((j));
					sorted_block.clear();
					while(!sorted_block.isFull() && heap.size>0){
						Tuple_with_position tp= heap.pop_min();
						sorted_block.appendTuple(tp.tuple);
					}
				}
				return_relation.setBlocks(i*Config.NUM_OF_BLOCKS_IN_MEMORY, 0, Config.NUM_OF_BLOCKS_IN_MEMORY);
			}
			else{
				return_relation.getBlocks(i*Config.NUM_OF_BLOCKS_IN_MEMORY, 0, num_blocks-i*Config.NUM_OF_BLOCKS_IN_MEMORY);
				for(int j=0;j<num_blocks-i*Config.NUM_OF_BLOCKS_IN_MEMORY;j++){
					Block sort_block=mem.getBlock(j);
					if(sort_block.isEmpty()) {continue;}
					for(int k=0;k<sort_block.getNumTuples();k++){
						Tuple sort_tuple=sort_block.getTuple(k);
						if(sort_tuple.isNull())  {continue;}
						Tuple_with_position tp=new Tuple_with_position(sort_tuple,0,0,0);
						heap.insert(tp);
					}
				}
				for(int j=0;j<num_blocks-i*Config.NUM_OF_BLOCKS_IN_MEMORY;j++){
					Block sorted_block=mem.getBlock((j));
					sorted_block.clear();
					while(!sorted_block.isFull() && heap.size>0){
						Tuple_with_position tp= heap.pop_min();
						sorted_block.appendTuple(tp.tuple);
					}
				}
				return_relation.setBlocks(i*Config.NUM_OF_BLOCKS_IN_MEMORY, 0, num_blocks-i*Config.NUM_OF_BLOCKS_IN_MEMORY);
			}
		}
		return return_relation;
	}

	private Relation distinct_second_pass(Relation return_relation, ArrayList<String> field_names){
		Heap heap=new Heap(80,field_names);
		int blocks=return_relation.getNumOfBlocks();
		int num_sublists=0;
		if(blocks%Config.NUM_OF_BLOCKS_IN_MEMORY==0)  num_sublists=blocks/Config.NUM_OF_BLOCKS_IN_MEMORY;
		else num_sublists=blocks/Config.NUM_OF_BLOCKS_IN_MEMORY+1;
		for(int i=0;i<num_sublists;i++){
			return_relation.getBlock(i*Config.NUM_OF_BLOCKS_IN_MEMORY, i);
		}
		for(int i=0;i<num_sublists;i++){
			Block tested_block=mem.getBlock(i);
			Tuple tested_tuple=tested_block.getTuple(0);
			Tuple_with_position tested_tuple_p=new Tuple_with_position(tested_tuple,i,0,0);
			heap.insert(tested_tuple_p);
		}
		if(schema_manager.relationExists("distinct_relation")){
			schema_manager.deleteRelation("distinct_relation");
		}
		Relation distinct_relation=schema_manager.createRelation("distinct_relation", return_relation.getSchema());
		Tuple_with_position output=heap.pop_min();
		Tuple output_tuple=output.tuple;
		heap.insert(output);
		appendTupleToRelation(distinct_relation, mem, 9, output_tuple);
		while(heap.size>0)//second pass of 2 pass
		{
			Tuple_with_position tp = heap.pop_min();
			if(heap.compare_tuple(tp.tuple, output.tuple) != 0)
			{
				appendTupleToRelation(distinct_relation, mem, 9, tp.tuple);
				output = tp;
			}
			if(tp.tuple_pointer<mem.getBlock(tp.sublist_pointer).getNumTuples()-1)
			{
				Tuple tuple = mem.getBlock(tp.sublist_pointer).getTuple(tp.tuple_pointer+1);
				heap.insert(new Tuple_with_position(tuple,tp.sublist_pointer,tp.block_pointer,tp.tuple_pointer+1));
			}
			else if(tp.block_pointer<9 && tp.sublist_pointer*10+tp.block_pointer<blocks-1){//sublist not exhaust
				tp.block_pointer++;
				return_relation.getBlock(tp.sublist_pointer*10+tp.block_pointer, tp.sublist_pointer);
				heap.insert(new Tuple_with_position(mem.getBlock(tp.sublist_pointer).getTuple(0),tp.sublist_pointer,tp.block_pointer,0));
			}
		}

		return distinct_relation;

	}

	private Relation order_second_pass(Relation return_relation, ArrayList<String> order_attr){
//		 if(return_relation.getSchema().getFieldNames().contains(order_attr.get(0))){
//			 Relation order_relation=schema_manager.createRelation("order_relation", return_relation.getSchema());
//			 return return_relation;
//		 }
		Heap heap=new Heap(80,order_attr);
		int blocks=return_relation.getNumOfBlocks();
		int num_sublists=0;
		if(blocks%Config.NUM_OF_BLOCKS_IN_MEMORY==0)  num_sublists=blocks/Config.NUM_OF_BLOCKS_IN_MEMORY;
		else num_sublists=blocks/Config.NUM_OF_BLOCKS_IN_MEMORY+1;
		for(int i=0;i<num_sublists;i++){
			return_relation.getBlock(i*Config.NUM_OF_BLOCKS_IN_MEMORY, i);
		}
		for(int i=0;i<num_sublists;i++){
			Block tested_block=mem.getBlock(i);
			Tuple tested_tuple=tested_block.getTuple(0);
			Tuple_with_position tested_tuple_p=new Tuple_with_position(tested_tuple,i,0,0);
			heap.insert(tested_tuple_p);
		}
		if(schema_manager.relationExists("order_relation")){
			schema_manager.deleteRelation("order_relation");
		}
		Relation order_relation=schema_manager.createRelation("order_relation", return_relation.getSchema());
		Tuple_with_position output=heap.pop_min();
		Tuple output_tuple=output.tuple;
		heap.insert(output);
		appendTupleToRelation(order_relation, mem, 9, output_tuple);
		while(heap.size>0)//second pass of 2 pass
		{
			Tuple_with_position tp = heap.pop_min();
			appendTupleToRelation(order_relation, mem, 9, tp.tuple);
			output = tp;
			if(tp.tuple_pointer<mem.getBlock(tp.sublist_pointer).getNumTuples()-1)
			{
				Tuple tuple = mem.getBlock(tp.sublist_pointer).getTuple(tp.tuple_pointer+1);
				heap.insert(new Tuple_with_position(tuple,tp.sublist_pointer,tp.block_pointer,tp.tuple_pointer+1));
			}
			else if(tp.block_pointer<9 && tp.sublist_pointer*10+tp.block_pointer<blocks-1){//sublist not exhaust
				tp.block_pointer++;
				return_relation.getBlock(tp.sublist_pointer*10+tp.block_pointer, tp.sublist_pointer);
				heap.insert(new Tuple_with_position(mem.getBlock(tp.sublist_pointer).getTuple(0),tp.sublist_pointer,tp.block_pointer,0));
			}
		}

		return order_relation;




	}

	private static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
		Block block_reference;
		if (relation_reference.getNumOfBlocks()==0) {
//		      System.out.print("The relation is empty" + "\n");
//		      System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
			block_reference=mem.getBlock(memory_block_index);
			block_reference.clear(); //clear the block
			block_reference.appendTuple(tuple); // append the tuple
//		      System.out.print("Write to the first block of the relation" + "\n");
			relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
		} else {
//		      System.out.print("Read the last block of the relation into memory block 5:" + "\n");
			relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
			block_reference=mem.getBlock(memory_block_index);

			if (block_reference.isFull()) {
//		        System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
				block_reference.clear(); //clear the block
				block_reference.appendTuple(tuple); // append the tuple
//		        System.out.print("Write to a new block at the end of the relation" + "\n");
				relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
			} else {
//		        System.out.print("(The block is not full: Append it directly)" + "\n");
				block_reference.appendTuple(tuple); // append the tuple
//		        System.out.print("Write to the last block of the relation" + "\n");
				relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
			}
		}
	}
}