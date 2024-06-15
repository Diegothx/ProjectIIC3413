#include <fstream>
#include <iostream>

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

struct UndoChanges {
    uint32_t transaction_id;
    uint32_t table_id;
    uint32_t page_num;
    uint32_t offset;
    uint32_t length;
    std::vector<char> old_data;
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

    auto system = System::init(db_directory, BufferManager::DEFAULT_BUFFER_SIZE);
    /*buffer_mgr
    FileManager& file_mgr      = reinterpret_cast<FileManager&>(file_mgr_buf);
    LogManager& log_mgr        = reinterpret_cast<LogManager&>(log_mgr_buf);
    Catalog& catalog */
    char* buffer = new char[Page::SIZE];
    log_file.read(buffer, 1);

    std::unordered_map<uint32_t, std::vector<UndoChanges>> pending_to_undo;
    while (log_file.good()) {
        LogType log_type = static_cast<LogType>(buffer[0]);
        switch (log_type) {
            case LogType::START: {
                break;
            }
            case LogType::COMMIT: {
                auto tid = read_uint32(log_file);
                pending_to_undo.erase(tid);
                break;
            }
            case LogType::ABORT: {
                auto tid = read_uint32(log_file);
                pending_to_undo.erase(tid);
                break;
            }
            case LogType::WRITE_U: {
                auto  tid = read_uint32(log_file);
                auto  table_id = read_uint32(log_file);
                auto  page_num = read_uint32(log_file);
                auto  offset = read_uint32(log_file);
                auto  len = read_uint32(log_file);

                std::vector<char> old_data(len);
                log_file.read(old_data.data(), len);

                pending_to_undo[tid].push_back({tid, table_id, page_num, offset, len, old_data});
                break;
            }
            default: {
                std::cerr << "Unknown log type: " << static_cast<int>(log_type) << "\n";
                break;
            }
        }
    }
    delete[] buffer;

    for (const auto& [tid, undos] : pending_to_undo) {
        for (const auto& undo : undos) {
            FileId file_id = catalog.get_file_id(undo.table_id);
            Page &page = buffer_mgr.get_page(file_id, undo.page_num);

            page.data()[undo.offset] = undo.old_data[0];
            page.make_dirty();
        }
    }
}
