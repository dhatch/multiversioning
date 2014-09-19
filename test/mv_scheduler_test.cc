#include <gtest/gtest.h>
#include <preprocessor.h>
#include <database.h>

#include <cstdlib>
#include <unordered_map>
#include <time.h>

using namespace std;

class SchedulerTest : public testing::Test {
	
protected:
	
	MVScheduler *sched;
	int numRecords;
	unordered_map<uint64_t, uint64_t> versionTracker;
	unordered_map<uint64_t, Action*> actionTracker;

	virtual void SetUp() {		
		InitTable();

        MVSchedulerConfig config = {
            0, 
            0,
            (1 << 20),
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
        };

		sched = new MVScheduler(config);
		sched->txnCounter = 1;
	}

	virtual void SchedWrapper(Action *action) {
		sched->ScheduleTransaction(action);
	}

	virtual void InitTable() {
		MVRecordAllocator *recordAlloc = 
			new MVRecordAllocator((1<<20)*sizeof(MVRecord));
		ASSERT_TRUE(recordAlloc != NULL);

		MVTablePartition *part = new MVTablePartition((1 << 16), recordAlloc);
		ASSERT_TRUE(part != NULL);

		MVTablePartition **partArray = 
			(MVTablePartition**)malloc(sizeof(MVTable*));
		partArray[0] = part;

		MVTable *tbl = new MVTable(1, partArray);
		bool success = DB.PutTable(0, tbl);
		ASSERT_TRUE(success);
		
		numRecords = 100;
		CompositeKey temp;
		for (int i = 0; i < numRecords; ++i) {
			temp.tableId = 0;
			temp.key = (uint64_t)i;
			temp.version = 0;

			bool success = false;
			success = tbl->WriteNewVersion(0, temp, NULL, 0);
			ASSERT_TRUE(success);
			versionTracker[(uint64_t)i] = 0;
			actionTracker[(uint64_t)i] = NULL;
		}
	}
};

TEST_F(SchedulerTest, Test) {
	Action toSchedule[1000];
	int writesetSize = 1;
	memset(toSchedule, 0, sizeof(Action)*1000);
	srand(time(NULL));	

	CompositeKey temp;
	for (int i = 0; i < 1000; ++i) {
		for (int j = 0; j < writesetSize; ++j) {
			uint64_t key = (uint64_t)((i + j)%numRecords);
			temp.tableId = 0;
			temp.key = key;
			temp.version = 0;

			toSchedule[i].readset.push_back(temp);
			toSchedule[i].writeset.push_back(temp);
		}

		SchedWrapper(&toSchedule[i]);
		
		for (int j = 0; j < writesetSize; ++j) {
			uint64_t key = toSchedule[i].readset[j].key;
			ASSERT_EQ(versionTracker[key], toSchedule[i].readset[j].version);

			/*
			MVTable *tbl;
			bool success = DB.GetTable(0, &tbl);
			ASSERT_TRUE(success);
			MVRecord *prevVersion = 
			*/

			versionTracker[key] = toSchedule[i].version;
			actionTracker[key] = &toSchedule[i];
		}		
	}
}
