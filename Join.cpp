#include "Join.hpp"
#include <iostream>
#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {

	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));
	Page* inputPage = mem -> mem_page(MEM_SIZE_IN_PAGE - 1); // pointer to the page in memory where we are pulling pages
	
	// LEFT RELATION
	for (uint pageid = left_rel.first; pageid < left_rel.second; pageid++){
		mem -> loadFromDisk(disk, pageid, MEM_SIZE_IN_PAGE - 1); // load each page into memory specifically in last page
		
		for (uint recordId = 0; recordId < inputPage -> size(); recordId++){

			Record currRecord = inputPage -> get_record(recordId);
			
			int hashedBucket = currRecord.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			Page* hashedPage = mem -> mem_page(hashedBucket);

			if (hashedPage -> full()){ // flush page bc it is full
				int writtenPageId = mem -> flushToDisk(disk, hashedBucket);
				partitions[hashedBucket].add_left_rel_page(writtenPageId);
			}

			// add record to hash
			hashedPage -> loadRecord(currRecord);
		}
	}

	for (uint bucket = 0; bucket < MEM_SIZE_IN_PAGE - 1; bucket++){
		if (!(mem -> mem_page(bucket) -> empty())){ // if page isn't empty, flush it to disk
			int writtenPageId = mem -> flushToDisk(disk, bucket);
			partitions[bucket].add_left_rel_page(writtenPageId);
		}
	}

	// RIGHT RELATION
	for (uint pageid = right_rel.first; pageid < right_rel.second; pageid++){
		mem -> loadFromDisk(disk, pageid, MEM_SIZE_IN_PAGE - 1); // load each page into memory specifically in last page
		for (uint recordId = 0; recordId < inputPage -> size(); recordId++){

			Record currRecord = inputPage -> get_record(recordId);
			
			int hashedBucket = currRecord.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			Page* hashedPage = mem -> mem_page(hashedBucket);

			if (hashedPage -> full()){ // flush page bc it is full
				int writtenPageId = mem -> flushToDisk(disk, hashedBucket);
				partitions[hashedBucket].add_right_rel_page(writtenPageId);
			}

			// add record to hash
			hashedPage -> loadRecord(currRecord);
		}
	}
	
	for (uint bucket = 0; bucket < MEM_SIZE_IN_PAGE - 1; bucket++){
		if (!(mem -> mem_page(bucket) -> empty())){ // if page isn't empty, flush it to disk
			int writtenPageId = mem -> flushToDisk(disk, bucket);
			partitions[bucket].add_right_rel_page(writtenPageId);
		}
	}

	mem -> reset();
	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	vector<uint> disk_pages; // placeholder

	Page* inputPage = mem -> mem_page(MEM_SIZE_IN_PAGE - 2);
	Page* outputPage = mem -> mem_page(MEM_SIZE_IN_PAGE - 1);

	for (uint bucket = 0; bucket < MEM_SIZE_IN_PAGE - 1; bucket++){
		
		for (uint pageid : partitions[bucket].get_left_rel()){
			mem -> loadFromDisk(disk, pageid, MEM_SIZE_IN_PAGE - 2); // load into input page
			
			for (uint leftRecordId = 0; leftRecordId < inputPage -> size(); leftRecordId++){
				Record leftRecord = inputPage -> get_record(leftRecordId);
				uint hashBucket = leftRecord.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
				Page* hashedPage = mem -> mem_page(hashBucket);
				hashedPage -> loadRecord(leftRecord);
			}
		}

		inputPage -> reset();

		for (uint pageid : partitions[bucket].get_right_rel()){
			mem -> loadFromDisk(disk, pageid, MEM_SIZE_IN_PAGE - 2); // load into input page
			
			for (uint rightRecordId = 0; rightRecordId < inputPage -> size(); rightRecordId++){
				Record rightRecord = inputPage -> get_record(rightRecordId);
				uint hashBucket = rightRecord.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
				Page* hashedPage = mem -> mem_page(hashBucket);
				
				for (uint leftRecordId = 0; leftRecordId < hashedPage -> size(); leftRecordId++){
					Record leftRecord = hashedPage -> get_record(leftRecordId);
					if (leftRecord == rightRecord){
						if (outputPage -> full()){
							uint writtenPage = mem -> flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
							disk_pages.push_back(writtenPage);
						}

						outputPage -> loadPair(leftRecord, rightRecord);
					}
				}
			}
		}

		for (uint bucket = 0; bucket < MEM_SIZE_IN_PAGE - 2; bucket++){
			mem -> mem_page(bucket) -> reset(); // clear 2nd hash function after each partition
		}
	}

	if (!(outputPage -> empty())){
		uint writtenPage = mem -> flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
		disk_pages.push_back(writtenPage);
	}

	return disk_pages;
}