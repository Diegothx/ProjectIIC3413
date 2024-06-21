#include <fstream>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <deque>
#include <list>
#include <memory>
#include <tuple>
#include <algorithm> 

#include "relational_model/system.h"
#include "third_party/cli11/CLI11.hpp"

uint32_t read_uint32(std::fstream& log_file) {
    uint32_t res;

    char small_buffer[4];
    log_file.read(small_buffer, 4);

    char* res_bytes = reinterpret_cast<char*>(&res);

    res_bytes[0] = small_buffer[0];
    res_bytes[1] = small_buffer[1];
    res_bytes[2] = small_buffer[2];
    res_bytes[3] = small_buffer[3];

    return res;
}

struct TransactionData {
    uint32_t table_id;
    uint32_t page_num;
    uint32_t offset;
    uint32_t len;
    std::unique_ptr<char[]> old_data;
    std::unique_ptr<char[]> new_data;
};


int main(int argc, char* argv[]) {
    std::string log_path;

    std::string db_directory;

    CLI::App app{"IIC 3413 DB"};
    app.get_formatter()->column_width(35);
    app.option_defaults()->always_capture_default();

    app.add_option("database", db_directory)
        ->description("Database directory")
        ->type_name("<path>")
        ->check(CLI::ExistingDirectory.description(""))
        ->required();

    app.add_option("log file", log_path)
        ->description("Log file")
        ->type_name("<path>")
        ->check(CLI::ExistingFile.description(""))
        ->required();

    CLI11_PARSE(app, argc, argv);

    std::fstream log_file(log_path, std::ios::binary|std::ios::in);

    if (log_file.fail()) {
        std::cerr << "Could not open the log at path: " << log_path << "\n";
        return EXIT_FAILURE;
    }

    char* buffer = new char[Page::SIZE];

    log_file.read(buffer, 1);
    std::unordered_map<uint32_t, std::deque<TransactionData>> active_transactions;
    std::list<uint32_t> committed_transactions;
    std::list<uint32_t> incomplete_transactions;
    while (log_file.good()) {
        LogType log_type = static_cast<LogType>(buffer[0]);
        switch (log_type) {
            case LogType::START: {
                auto tid = read_uint32(log_file);
                incomplete_transactions.push_back(tid);
                break;
            }
            case LogType::COMMIT: {
                auto tid = read_uint32(log_file);
                incomplete_transactions.remove(tid);
                committed_transactions.push_back(tid);
                break;
            }
            case LogType::ABORT: {
                auto tid = read_uint32(log_file);
                active_transactions.erase(tid);
                incomplete_transactions.remove(tid);
                committed_transactions.remove(tid);
                break;
            }
            case LogType::END: {
                auto tid = read_uint32(log_file);
                active_transactions.erase(tid);
                incomplete_transactions.remove(tid);
                committed_transactions.remove(tid);
                break;
            }
            case LogType::WRITE_UR: {
                auto tid = read_uint32(log_file);
                auto table_id = read_uint32(log_file);
                auto page_num = read_uint32(log_file);
                auto offset = read_uint32(log_file);
                auto len = read_uint32(log_file);

                auto old_data = std::make_unique<char[]>(len);
                log_file.read(old_data.get(), len);

                auto new_data = std::make_unique<char[]>(len);;
                log_file.read(new_data.get(), len);

                active_transactions[tid].push_back({table_id, page_num, offset, len, std::move(old_data), std::move(new_data)});
                break;
            }
            default: {
                std::cout << "Unknown log type: " << static_cast<int>(log_type) << "\n";
                break;
            }
        }
        log_file.read(buffer, 1);
    }

    delete[] buffer;
    auto system = System::init(db_directory, BufferManager::DEFAULT_BUFFER_SIZE);

    for (const auto& tid : incomplete_transactions) {
        std::cout << "Transaction " << tid << " is incomplete\n";
        if (active_transactions.find(tid) != active_transactions.end()) {
            while (!active_transactions[tid].empty()) {
                auto& undo = active_transactions[tid].back();
                std::cout << "UNDO: " << tid << " " << undo.table_id << " " << undo.page_num << " " << undo.offset << " " << undo.len << "\n";
                FileId file_id = catalog.get_file_id(undo.table_id);
                Page &page = buffer_mgr.get_page(file_id, undo.page_num);
                std::memcpy(page.data() + undo.offset, undo.old_data.get(), undo.len);
                page.make_dirty();
                page.unpin();
                active_transactions[tid].pop_back();
            }
        }
    }

    for (const auto& tid : committed_transactions) {
        std::cout << "Transaction " << tid << " is committed\n";
        if (active_transactions.find(tid) != active_transactions.end()) {
            while (!active_transactions[tid].empty()) {
                auto& redo = active_transactions[tid].front();
                std::cout << "REDO: " << tid << " " << redo.table_id << " " << redo.page_num << " " << redo.offset << " " << redo.len << "\n";
                FileId file_id = catalog.get_file_id(redo.table_id);
                Page &page = buffer_mgr.get_page(file_id, redo.page_num);
                std::memcpy(page.data() + redo.offset, redo.old_data.get(), redo.len);
                page.make_dirty();
                page.unpin();
                active_transactions[tid].pop_front();
            }
        }
    }
}

//<WRITE-UR,3,2,0,128,4,0x180d0000,0xffffffff>