/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import sys.dao.ProductoDAO;
import sys.model.AimarProductos;
import sys.model.ZProductoPrecio;

import sys.util.Service;

public class ProductoDAOImp implements ProductoDAO {

    public ProductoDAOImp() {
    }

    @Override
    public List<AimarProductos> listarProductos() {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarProductos producto = null;
        List<AimarProductos> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, Cod_Ai_Produc, Des_Ai_Produc, Est_Ai_Produc, "
                    + "CodNiv_1_Ai_Produc, CodNiv_2_Ai_Produc, CodNiv_3_Ai_Produc, "
                    + "CodNiv_4_Ai_Produc, CodNiv_5_Ai_Produc, fec_creacion, hor_creacion, "
                    + "cod_usuario_creacion, fec_actualizacion, hor_actualizacion, "
                    + "cod_usuario_actualizacion FROM AIMAR_Productos "
                    + "WHERE Cod_Ai_Produc NOT IN (SELECT Z_Producto_Precio.Cod_Producto "
                    + "FROM Z_Producto_Precio);";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                producto = new AimarProductos();
                producto.setRucCompanyia(rs.getString("ruc_companyia"));
                producto.setCodAiProduc(rs.getString("Cod_Ai_Produc"));
                producto.setDesAiProduc(rs.getString("Des_Ai_Produc"));
                producto.setEstAiProduc(rs.getString("Est_Ai_Produc"));
                producto.setCodNiv1AiProduc(rs.getString("CodNiv_1_Ai_Produc"));
                producto.setCodNiv2AiProduc(rs.getString("CodNiv_2_Ai_Produc"));
                producto.setCodNiv3AiProduc(rs.getString("CodNiv_3_Ai_Produc"));
                producto.setCodNiv4AiProduc(rs.getString("CodNiv_4_Ai_Produc"));
                producto.setCodNiv5AiProduc(rs.getString("CodNiv_5_Ai_Produc"));
                producto.setFecCreacion(rs.getString("fec_creacion"));
                producto.setHorCreacion(rs.getString("hor_creacion"));
                producto.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                producto.setFecActualizacion(rs.getString("fec_actualizacion"));
                producto.setHorActualizacion(rs.getString("hor_actualizacion"));
                producto.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));

                lista.add(producto);
            }
            if (!existe) {
                producto = null;
                System.out.println("No se encontro datos");
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return lista;

    }

    @Override
    public AimarProductos consultarObjProducto(String codProducto) {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AimarProductos producto = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, Cod_Ai_Produc, Des_Ai_Produc, Est_Ai_Produc, "
                    + "CodNiv_1_Ai_Produc, CodNiv_2_Ai_Produc, CodNiv_3_Ai_Produc, "
                    + "CodNiv_4_Ai_Produc, CodNiv_5_Ai_Produc, fec_creacion, hor_creacion, "
                    + "cod_usuario_creacion, fec_actualizacion, hor_actualizacion, "
                    + "cod_usuario_actualizacion FROM AIMAR_Productos WHERE Cod_Ai_Produc=?";

            ps = cn.prepareStatement(sql);
            ps.setString(1, codProducto);
            rs = ps.executeQuery();

            producto = new AimarProductos();
            if (rs.next()) {
                producto.setRucCompanyia(rs.getString("ruc_companyia"));
                producto.setCodAiProduc(rs.getString("Cod_Ai_Produc"));
                producto.setDesAiProduc(rs.getString("Des_Ai_Produc"));
                producto.setEstAiProduc(rs.getString("Est_Ai_Produc"));
                producto.setCodNiv1AiProduc(rs.getString("CodNiv_1_Ai_Produc"));
                producto.setCodNiv2AiProduc(rs.getString("CodNiv_2_Ai_Produc"));
                producto.setCodNiv3AiProduc(rs.getString("CodNiv_3_Ai_Produc"));
                producto.setCodNiv4AiProduc(rs.getString("CodNiv_4_Ai_Produc"));
                producto.setCodNiv5AiProduc(rs.getString("CodNiv_5_Ai_Produc"));
                producto.setFecCreacion(rs.getString("fec_creacion"));
                producto.setHorCreacion(rs.getString("hor_creacion"));
                producto.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                producto.setFecActualizacion(rs.getString("fec_actualizacion"));
                producto.setHorActualizacion(rs.getString("hor_actualizacion"));
                producto.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return producto;

    }
}
