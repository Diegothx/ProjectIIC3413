
#include <fstream>
#include <iostream>

#include <vector>
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
};

struct UndoTransactionChanges {
    int tid;
    TransactionData transaction_data;
};


void deleteTransactionsWithId(std::vector<UndoTransactionChanges>& touples, int idToDelete) {
    touples.erase(std::remove_if(touples.begin(), touples.end(),[idToDelete](const UndoTransactionChanges& tup) { return tup.tid == idToDelete; }),touples.end());
}


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

    std::fstream log_file(log_path, std::ios::binary |std::ios::in|std::ios::out);

    if (log_file.fail()) {
        std::cerr << "Could not open the log at path: " << log_path << "\n";
        return EXIT_FAILURE;
    }
    char* buffer = new char[Page::SIZE];

    // Check if the file is empty
    log_file.seekg(0, std::ios::end);
    if (log_file.tellg() == 0) {
        std::cerr << "Log file is empty.\n";
        return EXIT_FAILURE;
    }
    log_file.seekg(0, std::ios::beg);

    log_file.read(buffer, 1);
    std::vector<UndoTransactionChanges> pending_to_undo;
    uint64_t start_position = 0;
    uint64_t log_cut = 0;

    while (log_file.good()) {
        LogType log_type = static_cast<LogType>(buffer[0]);
        switch (log_type) {
            case LogType::START: {
                read_uint32(log_file);
                break;
            }
            case LogType::COMMIT:
            case LogType::ABORT:
                {
                    auto tid = read_uint32(log_file);
                    deleteTransactionsWithId(pending_to_undo, tid);
                }
                break;
            case LogType::WRITE_U: {
                int  tid = read_uint32(log_file);
                auto  table_id = read_uint32(log_file);
                auto  page_num = read_uint32(log_file);
                auto  offset = read_uint32(log_file);
                auto  len = read_uint32(log_file);

                auto old_data = std::make_unique<char[]>(len);
                log_file.read(old_data.get(), len);
                pending_to_undo.insert(pending_to_undo.begin(), {tid, {table_id, page_num, offset, len, std::move(old_data)}});
                break;
            }
            case LogType::START_CHKP: {
                start_position = log_file.tellg() - std::streampos(1);
                auto n = read_uint32(log_file);
                for (size_t i = 0; i < n; i++) {
                    read_uint32(log_file);
                }
                break;
            }
            case LogType::END_CHKP: {
                log_cut = start_position;
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

    for (auto& [tid, undo] : pending_to_undo) {
        std::cout << "Undoing Write-U," << tid << "," << undo.table_id << "," << undo.page_num << "," << undo.offset << "," << undo.len << "\n";
        FileId file_id = catalog.get_file_id(undo.table_id);
        Page &page = buffer_mgr.get_page(file_id, undo.page_num);
        page.data()[undo.offset] = std::move(undo.old_data[0]);
        page.make_dirty();
        page.unpin();
    }
    std::cout << "Undo completed cutting log file at position: " << log_cut << "\n";
    log_file.clear();
    log_file.seekp(log_cut, std::ios::beg);
    std::vector<char> buffer_file((std::istreambuf_iterator<char>(log_file)), std::istreambuf_iterator<char>());
    log_file.close();
    
    std::ofstream truncate_file(log_path, std::ios::binary | std::ios::out | std::ios::trunc);
    truncate_file.write(buffer_file.data(), buffer_file.size());
    truncate_file.close();

    log_file.close();
}